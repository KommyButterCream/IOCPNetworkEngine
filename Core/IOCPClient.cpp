#include "IOCPClient.h"

#include <WinSock2.h>

#include "../Diagnostics/EngineAssert.h"
#include <MSWSock.h> // for ConnectEx
#include <ws2tcpip.h> // for inet_pton

#include "IOCPCore.h"


#include "../Job/Job.h"
#include "../Scheduler/ClientSessionScheduler.h"
#include "../HandlerTable/PacketHandlerTable.h"

#include "../Network/SocketOption.h"

#include "../Memory/EngineMemoryPool.h"
#include "../Memory/EngineMemoryPoolHelper.h"

#include "../Buffer/RecvPacketBuffer.h"
#include "../Buffer/HybridSendPacketPool.h"
#include "../Buffer/PreDefine.h"
#include "../Protocol/PacketID.h"
#include "../Protocol/SystemPacket.h"

#include "../Session/ClientSession.h"
#include "../Session/SessionManager.h"

#include "../../Core/Util/Logger.h"

#pragma comment(lib, "ws2_32.lib") // for WinSock2
#pragma comment(lib, "mswsock.lib") // for AcceptEX / ConnectEx

using namespace Core::Util;

IOCPClient::IOCPClient()
{
}

IOCPClient::~IOCPClient()
{
	StopClient();
}

bool IOCPClient::StartClient(const char* serverIp, const uint16_t port)
{
	strcpy_s(m_serverIPAddress, sizeof(m_serverIPAddress), serverIp);
	m_serverPort = port;

	// 워커가 1개면 완료 핸들러 안에서 블로킹하는 순간 데드락이 된다.
	// 엔진의 disconnect 경로(OnDisconnectRequest)는 CancelPendingIO 후
	// WaitForIOCancelComplete 로 최대 10초를 기다리는데, 그 대기를 풀어 줄
	// 취소 완료 통지를 처리할 스레드가 바로 그 블로킹된 워커 자신이다.
	// 최소 2개를 두어 한 워커가 대기 중이어도 다른 워커가 완료를 처리하게 한다.
	//
	// 근본 해결은 disconnect 를 동기 대기 없이 refcount 기반으로 지연 처리하는 것이다.
	SetIOCPThreadCount(2);

	if (!IOCPCore::Start())
	{
		return false;
	}

	// 클라이언트 소켓 생성
	if (!CreateConnectSocket())
	{
		return false;
	}

	// GQCS 사용을 위한 포트와 소켓 바인드
	if (!BindClientSocket(m_clientSocket))
	{
		return false;
	}

	// ConnectEx 사용을 위한 함수 포인터 초기화
	if (!InitializeGUIDConnectEx(m_clientSocket))
	{
		return false;
	}

	constexpr size_t JobObjectSize = sizeof(Job);
	constexpr size_t AlignedJobObjectSize = (JobObjectSize + 63) & ~63;
	EngineMemoryPool::SlabConfig configsJob[] = {
		{AlignedJobObjectSize, 1024}
	};

	m_jobMemoryPool = new EngineMemoryPool;
	if (!m_jobMemoryPool)
		return false;

	// Job 은 __declspec(align(64)) 이므로 페이로드도 64바이트 정렬이어야 한다.
	// 예전 풀은 16바이트만 보장해서 4개 중 1개만 실제로 정렬되어 있었다.
	if (!m_jobMemoryPool->Initialize(configsJob, _countof(configsJob), alignof(Job)))
	{
		LOGE("failed to initialize the job memory pool");
		return false;
	}

	EngineMemoryPool::SlabConfig configsPacket[] = {
		{64, 1024},
		{128, 1024},
		{256, 1024},
		{512, 1024},
		{MEMORY_SIZE_1K, 512},
		{MEMORY_SIZE_2K, 512},
		{MEMORY_SIZE_4K, 512},
		{MEMORY_SIZE_8K, 256},
		{MEMORY_SIZE_16K, 128},
		{MEMORY_SIZE_32K, 128},
	};

	m_packetMemoryPool = new EngineMemoryPool;
	if (!m_packetMemoryPool)
		return false;

	m_packetMemoryPool->Initialize(configsPacket, _countof(configsPacket));

	EngineMemoryPool::SlabConfig configsImageBuffer[] = {
	{MEMORY_SIZE_1MB, 1},
	//{MEMORY_SIZE_4MB, 100},
	//{MEMORY_SIZE_8MB, 50}
	};

	m_generalMemoryPool = new EngineMemoryPool;
	if (!m_generalMemoryPool)
		return false;

	m_generalMemoryPool->Initialize(configsImageBuffer, _countof(configsImageBuffer));

	// PacketSend 를 위한 패킷 버퍼 풀 생성(1개)
	m_hybridSendPacketPool = new HybridSendPacketPool();
	if (!m_hybridSendPacketPool)
		return false;
	if (!m_hybridSendPacketPool->Initialize(HYBRID_SEND_PACKET_POOL_SIZE, 1))
		return false;

	m_handlerContext.jobMemoryPool = GetJobMemoryPool();
	m_handlerContext.packetMemoryPool = GetPacketMemoryPool();
	m_handlerContext.generalMemoryPool = GetGeneralMemoryPool();
	m_handlerContext.serviceContext = GetServiceContext();

	m_packetHandlerTable = new PacketHandlerTable(m_handlerContext);
	if (!m_packetHandlerTable)
		return false;


	// 서버와의 통신을 위한 세션 생성
	m_session = new ClientSession;
	if (!m_session)
		return false;
	m_session->Initialize(SESSION_ROLE::CLIENT, 0);
	if (!m_session->InitializeMemoryPool(m_hybridSendPacketPool, m_jobMemoryPool, m_packetMemoryPool, m_generalMemoryPool))
		return false;
	m_session->SetEventHandler(this);

	// 세션에 소켓 설정
	m_session->SetClientSocket(m_clientSocket);

	// 세션 잡큐의 Job 을 처리 하기 위한 스케쥴러/스레드 생성
	m_clientSessionScheduler = new ClientSessionScheduler;
	if (!m_clientSessionScheduler)
		return false;

	m_clientSessionScheduler->Initialize(m_session, m_jobMemoryPool, m_packetMemoryPool, m_generalMemoryPool);

	// 비동기 Connect To Server
	// 연결에 대한 통지를 GQCS 에서 처리 한다.
	if (!PrepareConnect())
	{
		return false;
	}

	return true;
}

void IOCPClient::StopClient()
{
	if (::InterlockedCompareExchange(&m_destroyFlag, 1, 0) == 1)
		return;

	LOGI("client shutting down");

	if (m_clientSessionScheduler)
	{
		delete m_clientSessionScheduler;
		m_clientSessionScheduler = nullptr;
	}

	// [1] I/O 취소 요청 및 취소 완료 대기.
	//     이 대기는 IOCP 워커 스레드가 DecrementIO 에서 이벤트를 Set 해주어야 풀리므로
	//     반드시 IOCPCore::Stop() 보다 먼저 수행되어야 한다.
	if (m_session)
	{
		OnDisconnectRequest(m_session);
	}

	// [2] IOCP 정지. GQCS 워커 스레드를 조인한다.
	//     이 시점 이후로는 완료 통지가 발생하지 않으므로
	//     아래의 세션 및 메모리풀 해제가 안전해진다.
	IOCPCore::Stop();

	// [3] 세션 해제
	if (m_session)
	{
		delete m_session;
		m_session = nullptr;
	}

	if (m_jobMemoryPool)
	{
		delete m_jobMemoryPool;
		m_jobMemoryPool = nullptr;
	}

	if (m_generalMemoryPool)
	{
		delete m_generalMemoryPool;
		m_generalMemoryPool = nullptr;
	}

	if (m_packetMemoryPool)
	{
		delete m_packetMemoryPool;
		m_packetMemoryPool = nullptr;
	}

	if (m_hybridSendPacketPool)
	{
		m_hybridSendPacketPool->Finalize();
		delete m_hybridSendPacketPool;
		m_hybridSendPacketPool = nullptr;
	}

	if (m_packetHandlerTable)
	{
		delete m_packetHandlerTable;
		m_packetHandlerTable = nullptr;
	}

	memset(m_serverIPAddress, 0, sizeof(m_serverIPAddress));
	m_serverPort = 0;

	FinalizeGUIDConnectEx();
}

void IOCPClient::HandleCompletion(ULONG_PTR completionKey, LPOVERLAPPED overlapped, DWORD bytesTransferred, BOOL completionStatus)
{
	ISession* session = reinterpret_cast<ISession*>(completionKey);
	OverlappedEx* overlappedEx = reinterpret_cast<OverlappedEx*>(overlapped);

	// GQCS 자체가 실패하면 overlapped 가 nullptr 로 온다.
	// 종료 중 IOCP 핸들이 닫히는 경우가 대표적이다.
	if (session == nullptr || overlappedEx == nullptr)
	{
		LOGW("completion arrived with session %p overlapped %p (status %d, error %lu)",
			session, overlappedEx, completionStatus, ::GetLastError());
		return;
	}

	if (!completionStatus)
	{
		HandleSocketError(overlappedEx, session, ::WSAGetLastError(), overlappedEx->operation);

		return;
	}

	switch (overlappedEx->operation)
	{
	case IO_OPERATION::CONNECT:
		HandleConnect(overlappedEx->sessionId, bytesTransferred);
		break;
	case IO_OPERATION::RECV:
		HandleRecv(overlappedEx, session, bytesTransferred);
		break;
	case IO_OPERATION::SEND:
		HandleSend(overlappedEx, session, bytesTransferred);
		break;
	default:
		ENGINE_VIOLATION("completion arrived with an unknown io operation %d", static_cast<int>(overlappedEx->operation));
		break;
	}
}

void IOCPClient::HandleSocketError(OverlappedEx* overlappedEx, ISession* session, int errorCode, IO_OPERATION ioOperation)
{
	LOGW("session %u socket error %d on io %d", session->GetSessionID(), errorCode, (int)ioOperation);

	// 공용으로 처리 되어야 하는 예외 처리

	if (ioOperation == IO_OPERATION::CONNECT)
	{
		switch (errorCode)
		{
		case ERROR_HOST_UNREACHABLE:   // 호스트 접근 불가
		case ERROR_CONNECTION_REFUSED: // 서버가 연결 거부
		case ERROR_NETWORK_UNREACHABLE: // 네트워크 단절
		case ERROR_PORT_UNREACHABLE: // 서버가 갑자기 죽었거나 리셋한 경우
		case WSAECONNRESET: // SYN 보내는 도중 서버가 포트를 닫은 경우
		case WSAHOST_NOT_FOUND: // ConnectEx 이전 단계에서 발생된 문제 1
		case WSATRY_AGAIN:  // ConnectEx 이전 단계에서 발생된 문제 2
		case WSANO_DATA:  // ConnectEx 이전 단계에서 발생된 문제 3
		case WSAEADDRNOTAVAIL: // 로컬 인터페이스가 해당 주소 패밀리를 지원하지 않을 때
		case WSAEADDRINUSE: // 중복 포트 바인딩
			HandleConnectCancelled(overlappedEx, session);
			break;
		case ERROR_OPERATION_ABORTED:
			HandleConnectCancelled(overlappedEx, session);
			break;
		default:
			// 분류되지 않은 에러 코드. 그냥 빠져나가면 IncrementIO 로 올려둔
			// 카운트가 내려가지 않아 이 세션이 영구히 취소 대기에 묶인다.
			// 취소 처리로 보내 카운트를 정리한다.
			ENGINE_VIOLATION("session %u unhandled connect error %d, treating it as a cancellation", session->GetSessionID(), errorCode);
			HandleConnectCancelled(overlappedEx, session);
			break;
		}
		return;
	}
	else if (ioOperation == IO_OPERATION::RECV)
	{
		switch (errorCode)
		{
		case WSAECONNRESET:       // 연결이 비정상 종료됨 (상대방 강제 종료)
		case WSAECONNABORTED:     // 연결 중단됨
		case WSAENOTCONN:         // 연결이 이미 끊김
		case WSAESHUTDOWN:        // 소켓 송수신 불가
		case ERROR_NETNAME_DELETED: // 네트워크 이름 삭제됨 (연결 끊김)
		case ERROR_CONNECTION_ABORTED:
			// 서버와의 연결이 끊긴 상황
			HandleRecvCancelled(overlappedEx, session);
			break;
		case ERROR_OPERATION_ABORTED:
			HandleRecvCancelled(overlappedEx, session);
			break;
		default:
			// 분류되지 않은 에러 코드. 빠져나가면 IO 카운트가 누출되므로
			// 취소 처리로 보내 카운트를 정리한다.
			ENGINE_VIOLATION("session %u unhandled recv error %d, treating it as a cancellation", session->GetSessionID(), errorCode);
			HandleRecvCancelled(overlappedEx, session);
			break;
		}

		return;
	}
	else if (ioOperation == IO_OPERATION::SEND)
	{
		switch (errorCode)
		{
		case WSAECONNRESET:       // 연결이 비정상 종료됨 (상대방 강제 종료)
		case WSAECONNABORTED:     // 연결 중단됨
		case WSAENOTCONN:         // 연결이 이미 끊김
		case WSAESHUTDOWN:        // 소켓 송수신 불가
		case ERROR_NETNAME_DELETED: // 네트워크 이름 삭제됨 (연결 끊김)
			HandleSendCancelled(overlappedEx, session);
			break;
		case ERROR_OPERATION_ABORTED:
			HandleSendCancelled(overlappedEx, session);
			break;
		default:
			// 분류되지 않은 에러 코드. 빠져나가면 IO 카운트가 누출되므로
			// 취소 처리로 보내 카운트를 정리한다.
			ENGINE_VIOLATION("session %u unhandled send error %d, treating it as a cancellation", session->GetSessionID(), errorCode);
			HandleSendCancelled(overlappedEx, session);
			break;
		}

		return;
	}
	else
	{
		ENGINE_VIOLATION("socket error reported for an unknown io operation %d (error %d)", static_cast<int>(ioOperation), errorCode);
		return;
	}
}

void IOCPClient::HandleConnect(uint32_t sessionId, DWORD bytesTransferred)
{
	ENGINE_CHECK_RETVOID(m_session != nullptr, "connect completion arrived but there is no session");

	// ConnectEx 에 대한 IO 카운트를 먼저 내려놓는다.
	//
	// 이전에는 SetClientContext / SetNoDelay 가 실패하면 DecrementIO 없이
	// 그대로 return 했다. 그러면 카운트가 1 에 머물러 이 세션은 이후
	// WaitForIOCancelComplete 에서 영구히 풀리지 않는다(10초 타임아웃 후 단정).
	// 실패 여부와 무관하게 완료 통지 1건은 소비했으므로 여기서 내린다.
	m_session->DecrementIO();

	if (!SocketOption::SetClientContext(m_clientSocket))
	{
		LOGE("session %u SO_UPDATE_CONNECT_CONTEXT failed, dropping the connection", m_session->GetSessionID());
		OnDisconnectRequest(m_session);
		return;
	}

	if (!SocketOption::SetNoDelay(m_clientSocket))
	{
		LOGE("session %u TCP_NODELAY failed, dropping the connection", m_session->GetSessionID());
		OnDisconnectRequest(m_session);
		return;
	}

	if (!m_session->OnConnect())
	{
		LOGE("session %u OnConnect failed, dropping the connection", m_session->GetSessionID());
		OnDisconnectRequest(m_session);
		return;
	}

	if (!SendSystemAuthRequest(m_session))
	{
		LOGE("session %u system auth request send failed", m_session->GetSessionID());
		OnDisconnectRequest(m_session);
		return;
	}

	OnClientConnect(m_session);
}

void IOCPClient::HandleConnectCancelled(OverlappedEx* overlappedEx, ISession* session)
{
	LOGW("ConnectEx was cancelled");

	ClientSession* clientSession = dynamic_cast<ClientSession*>(session);

	if (!clientSession)
	{
		LOGE("client session cast failed : the session is not a ClientSession");

		return;
	}

	clientSession->SetClientSessionState(ClientSessionState::CONNECT_ABORTED);
	clientSession->DecrementIO(); // 접속 실패 했으니 ConnectEx 에 대한 IO 1개 감소

	OnClientDisconnect(clientSession);

	OnDisconnectRequest(clientSession);
}

void IOCPClient::HandleRecv(OverlappedEx* overlappedEx, ISession* session, DWORD bytesTransferred)
{
	if (!session)
		return;

	if (!overlappedEx)
		return;

	if (bytesTransferred == 0)
	{
		// 클라이언트쪽에서 소켓 Close 를 보낸경우
		// 정상적인 클라이언트 접속 종료
		const DWORD error = ::GetLastError();
		const int nError = ::WSAGetLastError();

		session->DecrementIO(); // 접속 실패 했으니 ConnectEx 에 대한 IO 1개 감소

		HandleSessionDisconnected(overlappedEx, session, bytesTransferred);

		return;
	}

	ClientSession* clientSession = static_cast<ClientSession*>(session);

	// DecrementIO 는 이 함수 끝에서 한다. 서버 쪽 HandleRecv 와 같은 이유다.
	// 카운트를 먼저 내리면 "카운트 0 = 아무도 세션을 만지지 않음" 이 깨지고,
	// 그 틈에 다른 스레드가 취소 대기를 통과해 세션을 정리할 수 있다.
	bool disconnectAfterHandling = false;

	clientSession->UpdateLastRecvTick();

	RecvPacketBuffer& recvBuf = clientSession->GetReceiveBuffer();

	if (!recvBuf.CommitWrite(bytesTransferred))
	{
		ENGINE_VIOLATION("session %u recv ring commit failed : %lu bytes would overflow (stored %u / capacity %u)",
			clientSession->GetSessionID(), bytesTransferred,
			recvBuf.GetStoredSize(), RECV_PACKET_BUFFER_SIZE);

		clientSession->DecrementIO();
		return;
	}

	// 패킷 처리 루프
	while (!recvBuf.IsEmpty())
	{
		uint32_t packetSize = 0;
		uint16_t packetId = 0;
		char* packetDataByMemoryPool = nullptr;

		if (!recvBuf.ReadPacket(packetDataByMemoryPool, packetSize, packetId))
		{
			// 패킷이 아직 다 안 왔거나 오류
			break;
		}

		if (!IsValidPacketId(packetId))
		{
			// 스트림이 어긋났다. 디스패치하지 않고 연결을 끊는다.
			MEMORY_POOL::ReleasePacket(*GetPacketMemoryPool(), *GetGeneralMemoryPool(), packetDataByMemoryPool);
			LOGE("session %u received an invalid packet id %u (size %u). dropping the connection",
				clientSession->GetSessionID(), packetId, packetSize);
			disconnectAfterHandling = true;
			break;
		}

		if (IsSystemPacketId(packetId))
		{
			const bool handled = HandleSystemPacket(clientSession, packetId, packetDataByMemoryPool, packetSize);

			MEMORY_POOL::ReleasePacket(*GetPacketMemoryPool(), *GetGeneralMemoryPool(), packetDataByMemoryPool);

			if (!handled)
			{
				LOGE("session %u engine packet handling failed (PacketID : %u)", clientSession->GetSessionID(), packetId);
				disconnectAfterHandling = true;
				break;
			}

			continue;
		}

		if (!clientSession->IsEstablished())
		{
			MEMORY_POOL::ReleasePacket(*GetPacketMemoryPool(), *GetGeneralMemoryPool(), packetDataByMemoryPool);
			LOGW("session %u service packet received before session established (PacketID : %u)", clientSession->GetSessionID(), packetId);
			disconnectAfterHandling = true;
			break;
		}

		// 패킷 완성! 실제 처리 호출
		OnReceive(clientSession, packetId, packetDataByMemoryPool, packetSize);
	}

	if (!disconnectAfterHandling)
	{
		// 다시 다음 수신 요청
		// 실패하면 이 세션은 pending recv 가 없는 상태로 남아 통신이 조용히 멈추므로
		// 그대로 방치하지 않고 연결을 정리한다.
		if (!clientSession->PostReceive())
		{
			LOGE("session %u failed to re-arm recv, disconnecting instead of going silent",
				clientSession->GetSessionID());
			disconnectAfterHandling = true;
		}
	}

	// 이 핸들러가 세션 사용을 끝냈으므로 이제 카운트를 내려놓는다.
	clientSession->DecrementIO();

	if (disconnectAfterHandling)
	{
		OnDisconnectRequest(clientSession);
	}
}

void IOCPClient::HandleRecvCancelled(OverlappedEx* overlappedEx, ISession* session)
{
	if (!session)
		return;

	if (!overlappedEx)
		return;

	// 걸어 두었던 WSARecv 에 대한 IO 감소
	session->DecrementIO();

	LOGI("session %u recv cancelled", session->GetSessionID());
}

void IOCPClient::HandleSend(OverlappedEx* overlappedEx, ISession* session, DWORD bytesTransferred)
{
	if (!session)
		return;

	if (!overlappedEx)
		return;

	ClientSession* clientSession = static_cast<ClientSession*>(session);

	if (clientSession->GetSessionRole() != SESSION_ROLE::CLIENT)
	{
		ENGINE_VIOLATION("session %u send completion arrived but the role is %d, not CLIENT",
			clientSession->GetSessionID(), static_cast<int>(clientSession->GetSessionRole()));

		// 카운트를 반드시 내려놓아야 한다. 그러지 않으면 취소 대기가 풀리지 않는다.
		clientSession->DecrementIO();
		return;
	}

	// OnSendCompleted 가 다음 패킷의 WSASend 까지 발행하므로 처리 후에 내린다.
	clientSession->OnSendCompleted(bytesTransferred);

	clientSession->DecrementIO();
}

void IOCPClient::HandleSendCancelled(OverlappedEx* overlappedEx, ISession* session)
{
	if (!session)
		return;

	if (!overlappedEx)
		return;

	// 걸어 두었던 WSASend 에 대한 IO 감소
	session->DecrementIO();

	LOGI("session %u send cancelled", session->GetSessionID());
}

void IOCPClient::HandleSessionDisconnected(OverlappedEx* overlappedEx, ISession* session, DWORD bytesTransferred)
{
	// 세션의 정상 종료 시퀀스

	ENGINE_CHECK_RETVOID(session != nullptr && overlappedEx != nullptr,
		"disconnect handler called with session %p overlapped %p", session, overlappedEx);

	if (overlappedEx->operation == IO_OPERATION::RECV)
	{
		OnDisconnectRequest(session);
	}
	else
	{
		// 0바이트 완료는 RECV 에서만 발생한다.
		ENGINE_VIOLATION("session %u reported a zero byte completion on io %d, which should only happen for RECV",
			session->GetSessionID(), static_cast<int>(overlappedEx->operation));
	}
}

bool IOCPClient::CreateConnectSocket()
{
	m_clientSocket = ::WSASocket(AF_INET, SOCK_STREAM, IPPROTO_TCP, NULL, 0, WSA_FLAG_OVERLAPPED);

	if (m_clientSocket == INVALID_SOCKET)
	{
		//Log::log(LogLevel::LOG_ERROR, "[%s] WSASocket Connect socket failed");
		return false;
	}

	//Log::log(LogLevel::LOG_INFO, "[%s] WSASocket Connect socket success");

	return true;
}

void IOCPClient::DestroyConnectSocket()
{
	IOCPCore::CloseSocketHandle(m_session->DetachSocket());
}

bool IOCPClient::BindClientSocket(SOCKET clientSocket)
{
	SOCKADDR_IN localAddr;
	localAddr.sin_family = AF_INET;
	localAddr.sin_port = 0;
	localAddr.sin_addr.s_addr = ::htonl(INADDR_ANY);

	if (::bind(clientSocket, (SOCKADDR*)&localAddr, sizeof(localAddr)) == SOCKET_ERROR)
	{
		//Log::log(LogLevel::LOG_ERROR, "[%s] bind failed - ErroCode : %d", WSAGetLastError());

		return false;
	}

	//Log::log(LogLevel::LOG_INFO, "[%s] bind success");

	return true;
}

bool IOCPClient::InitializeGUIDConnectEx(SOCKET clientSocket)
{
	// ConnectEx 를 사용하기 위해 함수 포인터를 얻어오는 함수
	// WSAIoctl 를 사용하여 ConnectEx 함수 포인터를 얻어올 수 있다.

	DWORD bytes = 0;

	// ConnectEX 함수 포인터 얻기
	GUID GuidConnectEx = WSAID_CONNECTEX;
	if (::WSAIoctl(clientSocket,
		SIO_GET_EXTENSION_FUNCTION_POINTER,
		&GuidConnectEx,
		sizeof(GuidConnectEx),
		&m_connectEx,
		sizeof(m_connectEx),
		&bytes,
		nullptr,
		nullptr) == SOCKET_ERROR)
	{
		//Log::log(LogLevel::LOG_ERROR, "[%s] WSAIoctl Get ConnectEx Pointer Failed - ErroCode : %d", WSAGetLastError());

		return false;
	}

	return true;
}

void IOCPClient::FinalizeGUIDConnectEx()
{
	m_connectEx = nullptr;
}

bool IOCPClient::PrepareConnect()
{
	if (!m_session)
	{
		//Log::log(LogLevel::LOG_ERROR, "[%s] Failed to acquire session");
		ENGINE_BREAK_IF_DEBUGGER();
		return false;
	}

	if (m_session->GetClientSessionState() != ClientSessionState::CONNECT_READY)
	{
		//Log::log(LogLevel::LOG_ERROR, "[%s] Session is not Ready");
		ENGINE_BREAK_IF_DEBUGGER();
		return false;
	}

	if (!PostConnect(m_session))
	{
		//Log::log(LogLevel::LOG_ERROR, "[%s] Failed to post ConnectEx");
		return false;
	}

	return true;
}

bool IOCPClient::PostConnect(ISession* session)
{
	ClientSession* clientSession = dynamic_cast<ClientSession*>(session);

	if (!clientSession)
	{
		LOGE("client session cast failed : the session is not a ClientSession");

		return false;
	}

	SOCKADDR_IN serveraddr = { 0 };
	serveraddr.sin_family = AF_INET;
	serveraddr.sin_port = htons(m_serverPort);
	int nResult = ::inet_pton(AF_INET, m_serverIPAddress, &serveraddr.sin_addr);

	if (nResult == 0)
	{
		ENGINE_VIOLATION("server address '%s' is not a valid IPv4 address", m_serverIPAddress);
		return false;
	}
	else if (nResult == -1)
	{
		int nError = ::WSAGetLastError();
		ENGINE_BREAK_IF_DEBUGGER();
	}

	// GQCS 사용을 위한 IOCP 등록
	if (!IOCPCore::RegisterSocketToIOCP((ULONG_PTR)session, session->GetClientSocket()))
		return false;

	// IO 수량 증가(ConnectEx 에 대한 IO)
	clientSession->IncrementIO();

	// Overlapped 구조체 초기화
	OverlappedEx& overlappedEx = clientSession->GetConnectOverlapped();  // session이 미리 생성한 OverlappedEx 포인터 반환

	overlappedEx.wsaBuffer.len = 0;
	overlappedEx.wsaBuffer.buf = nullptr;
	overlappedEx.operation = IO_OPERATION::CONNECT;
	overlappedEx.sessionId = clientSession->GetSessionID();
	clientSession->SetClientSessionState(ClientSessionState::CONNECTING);

	DWORD bytesSent = 0;
	BOOL bResult = m_connectEx(
		m_clientSocket,
		(SOCKADDR*)&serveraddr,
		sizeof(serveraddr),
		NULL,
		0,
		&bytesSent,
		(LPOVERLAPPED)&overlappedEx
	);

	if (bResult == FALSE)
	{
		const int nError = ::WSAGetLastError();
		if (nError != ERROR_IO_PENDING)
		{
			// ConnectEx 실패 했으므로 IO 수량 감소
			clientSession->DecrementIO(); // 접속 실패 했으니 ConnectEx 에 대한 IO 1개 감소
			clientSession->SetClientSessionState(ClientSessionState::CONNECT_ABORTED);

			OnClientDisconnect(clientSession);

			OnDisconnectRequest(clientSession);

			return false;
		}
	}

	return true;
}

void IOCPClient::OnDisconnectRequest(ISession* session)
{
	ClientSession* clientSession = dynamic_cast<ClientSession*>(session);

	if (!clientSession)
	{
		LOGE("client session cast failed : the session is not a ClientSession");

		return;
	}

	if (clientSession->GetClientSocket() != INVALID_SOCKET)
	{
		// Connect, Recv, Send IO 를 모두 취소하고
		if (!clientSession->CancelPendingIO())
		{
			ENGINE_VIOLATION("session %u CancelPendingIO failed during disconnect", clientSession->GetSessionID());
		}

		// 취소가 완료되기를 기다린다.
		if (!clientSession->WaitForIOCancelComplete(10'000))
		{
			// 취소가 끝나지 않았는데도 아래에서 소켓을 닫는다.
			// 남은 완료 통지가 닫힌 소켓을 참조할 수 있다.
			ENGINE_VIOLATION("session %u IO cancel did not complete, closing the socket anyway", clientSession->GetSessionID());
		}

		// 모든 IO 취소가 성공적으로 수행되었으면
		// 소켓을 닫는다.
		DestroyConnectSocket();
	}

	// 소켓을 닫은 이후 세션에 대한 상태 및 정리를 수행
	if (!clientSession->OnDisconnect())
	{
		ENGINE_VIOLATION("session %u OnDisconnect reported failure", clientSession->GetSessionID());
	}
}

PacketHandlerTable* IOCPClient::GetPacketHandlerTable() const
{
	return m_packetHandlerTable;
}

EngineMemoryPool* IOCPClient::GetJobMemoryPool() const
{
	return m_jobMemoryPool;
}

EngineMemoryPool* IOCPClient::GetPacketMemoryPool() const
{
	return m_packetMemoryPool;
}

EngineMemoryPool* IOCPClient::GetGeneralMemoryPool() const
{
	return m_generalMemoryPool;
}

const HandlerContext& IOCPClient::GetHandlerContext() const
{
	return m_handlerContext;
}

ClientSession* IOCPClient::GetClientSession() const
{
	return m_session;
}

void* IOCPClient::GetServiceContext()
{
	return this;
}

bool IOCPClient::SendSystemAuthRequest(ClientSession* session)
{
	if (!session || !session->IsTransportConnected())
	{
		return false;
	}

	void* memory = MEMORY_POOL::CreatePacket(*GetPacketMemoryPool(), sizeof(CS_SYSTEM_AUTH_REQUEST_PACKET));
	if (!memory)
	{
		return false;
	}

	CS_SYSTEM_AUTH_REQUEST_PACKET* request = reinterpret_cast<CS_SYSTEM_AUTH_REQUEST_PACKET*>(memory);
	*request = CS_SYSTEM_AUTH_REQUEST_PACKET();

	void* packetData = request;
	if (!session->EnqueueSendPacket(&packetData, sizeof(CS_SYSTEM_AUTH_REQUEST_PACKET)))
	{
		MEMORY_POOL::ReleasePacket(*GetPacketMemoryPool(), *GetGeneralMemoryPool(), request);
		return false;
	}

	session->SetClientSessionState(ClientSessionState::AUTH_PENDING);
	return true;
}

bool IOCPClient::HandleSystemPacket(ClientSession* session, uint16_t packetId, const char* packetData, uint32_t packetSize)
{
	if (!session || !packetData)
	{
		return false;
	}

	switch (static_cast<PACKET_ID>(packetId))
	{
	case PACKET_ID::SC_SYSTEM_AUTH_RESPONSE:
	{
		if (packetSize != sizeof(SC_SYSTEM_AUTH_RESPONSE_PACKET))
		{
			return false;
		}

		const SC_SYSTEM_AUTH_RESPONSE_PACKET* response = reinterpret_cast<const SC_SYSTEM_AUTH_RESPONSE_PACKET*>(packetData);
		const SYSTEM_AUTH_RESULT authResult = static_cast<SYSTEM_AUTH_RESULT>(response->authResult);

		if (response->protocolVersion != IOCP_ENGINE_PROTOCOL_VERSION)
		{
			LOGE("session %u protocol version mismatch (server=%u, client=%u)", session->GetSessionID(), response->protocolVersion, IOCP_ENGINE_PROTOCOL_VERSION);
			return false;
		}

		if (authResult != SYSTEM_AUTH_RESULT::SUCCESS)
		{
			LOGW("session %u auth rejected by server (result=%u)", session->GetSessionID(), response->authResult);
			return false;
		}

		session->SetClientSessionState(ClientSessionState::ESTABLISHED);
		LOGI("session %u session established", session->GetSessionID());
		OnSessionEstablished(session);
		return true;
	}

	case PACKET_ID::SC_SYSTEM_HEARTBEAT_REQUEST:
	{
		if (packetSize != sizeof(SC_SYSTEM_HEARTBEAT_REQUEST_PACKET))
		{
			return false;
		}

		if (!session->IsEstablished())
		{
			return false;
		}

		const SC_SYSTEM_HEARTBEAT_REQUEST_PACKET* request = reinterpret_cast<const SC_SYSTEM_HEARTBEAT_REQUEST_PACKET*>(packetData);
		return session->SendSystemHeartbeatResponse(request->tick);
	}

	default:
		LOGW("session %u unhandled system packet id: %u", session->GetSessionID(), packetId);
		return false;
	}
}
