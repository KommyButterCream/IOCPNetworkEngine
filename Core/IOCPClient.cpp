#include "IOCPClient.h"

#include <WinSock2.h>
#include <MSWSock.h> // for ConnectEx
#include <ws2tcpip.h> // for inet_pton

#include "IOCPCore.h"


#include "../Job/Job.h"
#include "../Scheduler/ClientSessionScheduler.h"
#include "../HandlerTable/PacketHandlerTable.h"

#include "../Network/SocketOption.h"

#include "../Memory/SlabMemoryPool.h"
#include "../Memory/SlabMemoryPoolHelper.h"

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

	SetIOCPThreadCount(1);

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
	SlabMemoryPool::SlabConfig configsJob[] = {
		{AlignedJobObjectSize, 1024}
	};

	m_jobMemoryPool = new SlabMemoryPool;
	if (!m_jobMemoryPool)
		return false;

	m_jobMemoryPool->Initialize(configsJob, _countof(configsJob));

	SlabMemoryPool::SlabConfig configsPacket[] = {
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

	m_packetMemoryPool = new SlabMemoryPool;
	if (!m_packetMemoryPool)
		return false;

	m_packetMemoryPool->Initialize(configsPacket, _countof(configsPacket));

	SlabMemoryPool::SlabConfig configsImageBuffer[] = {
	{MEMORY_SIZE_1MB, 1},
	//{MEMORY_SIZE_4MB, 100},
	//{MEMORY_SIZE_8MB, 50}
	};

	m_generalMemoryPool = new SlabMemoryPool;
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

	printf_s("%s\n", __FUNCTION__);

	if (m_clientSessionScheduler)
	{
		delete m_clientSessionScheduler;
		m_clientSessionScheduler = nullptr;
	}

	if (m_session)
	{
		OnDisconnectRequest(m_session);

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

	IOCPCore::Stop();
}

void IOCPClient::HandleCompletion(ULONG_PTR completionKey, LPOVERLAPPED overlapped, DWORD bytesTransferred, BOOL completionStatus)
{
	ISession* session = reinterpret_cast<ISession*>(completionKey);
	OverlappedEx* overlappedEx = reinterpret_cast<OverlappedEx*>(overlapped);

	if (session == nullptr || overlappedEx == nullptr)
	{
		__debugbreak();
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
		//Log::log(LogLevel::LOG_ERROR, "[%s] Unknown operation - ErrorCode: %d\n", __FUNCTION__, WSAGetLastError());
		break;
	}
}

void IOCPClient::HandleSocketError(OverlappedEx* overlappedEx, ISession* session, int errorCode, IO_OPERATION ioOperation)
{
	Logger::Log(LogLevel::LOG_INFO, "[%s][ClientSession : %d][IO : %d] 에러 핸들링", __FUNCTION__, session->GetSessionID(), (int)ioOperation);

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
			__debugbreak();
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
			__debugbreak();
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
			__debugbreak();
		}

		return;
	}
	else
	{
		__debugbreak();
		return;
	}
}

void IOCPClient::HandleConnect(uint32_t sessionId, DWORD bytesTransferred)
{
	if (!m_session)
	{
		__debugbreak();
		return;
	}

	if (!SocketOption::SetClientContext(m_clientSocket))
	{
		return;
	}

	if (!SocketOption::SetNoDelay(m_clientSocket))
	{
		return;
	}

	m_session->DecrementIO(); // 접속 성공 했으니 ConnectEx 에 대한 IO 1개 감소

	if (!m_session->OnConnect())
	{
		return;
	}

	if (!SendSystemAuthRequest(m_session))
	{
		Logger::Log(LogLevel::LOG_ERROR, "[%s][ClientSession : %d] system auth request send failed", __FUNCTION__, m_session->GetSessionID());
		OnDisconnectRequest(m_session);
		return;
	}

	OnClientConnect(m_session);
}

void IOCPClient::HandleConnectCancelled(OverlappedEx* overlappedEx, ISession* session)
{
	Logger::Log(LogLevel::LOG_WARNING, "[%s] ConnectEx IO Canceled", __FUNCTION__);

	ClientSession* clientSession = dynamic_cast<ClientSession*>(session);

	if (!clientSession)
	{
		Logger::Log(LogLevel::LOG_ERROR, "[%s] Client Session is nullptr", __FUNCTION__);

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

	ClientSession* clientSession = dynamic_cast<ClientSession*>(session);

	clientSession->DecrementIO();
	clientSession->UpdateLastRecvTick();

	RecvPacketBuffer& recvBuf = clientSession->GetReceiveBuffer();

	if (!recvBuf.CommitWrite(bytesTransferred))
	{
		printf_s("[Error] CommitWrite failed! Buffer overflow\n");
		__debugbreak();
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
			__debugbreak();
		}

		if (IsSystemPacketId(packetId))
		{
			if (!HandleSystemPacket(clientSession, packetId, packetDataByMemoryPool, packetSize))
			{
				MEMORY_POOL::ReleasePacket(*GetPacketMemoryPool(), *GetGeneralMemoryPool(), packetDataByMemoryPool);
				Logger::Log(LogLevel::LOG_ERROR, "[%s][ClientSession : %d] engine packet handling failed (PacketID : %u)", __FUNCTION__, clientSession->GetSessionID(), packetId);
				OnDisconnectRequest(clientSession);
				return;
			}

			MEMORY_POOL::ReleasePacket(*GetPacketMemoryPool(), *GetGeneralMemoryPool(), packetDataByMemoryPool);
			continue;
		}

		if (!clientSession->IsEstablished())
		{
			MEMORY_POOL::ReleasePacket(*GetPacketMemoryPool(), *GetGeneralMemoryPool(), packetDataByMemoryPool);
			Logger::Log(LogLevel::LOG_WARNING, "[%s][ClientSession : %d] service packet received before session established (PacketID : %u)", __FUNCTION__, clientSession->GetSessionID(), packetId);
			OnDisconnectRequest(clientSession);
			return;
		}

		// 패킷 완성! 실제 처리 호출
		OnReceive(clientSession, packetId, packetDataByMemoryPool, packetSize);
	}

	// 다시 다음 수신 요청
	bool bResult = clientSession->PostReceive();
}

void IOCPClient::HandleRecvCancelled(OverlappedEx* overlappedEx, ISession* session)
{
	if (!session)
		return;

	if (!overlappedEx)
		return;

	// 걸어 두었던 WSARecv 에 대한 IO 감소
	session->DecrementIO();

	printf_s("[IOCPClient] :: [Session ID : %d] Recv Cancelled.\n", session->GetSessionID());
}

void IOCPClient::HandleSend(OverlappedEx* overlappedEx, ISession* session, DWORD bytesTransferred)
{
	if (!session)
		return;

	if (!overlappedEx)
		return;

	ClientSession* clientSession = dynamic_cast<ClientSession*>(session);

	if (clientSession->GetSessionRole() != SESSION_ROLE::CLIENT)
	{
		Logger::Log(LogLevel::LOG_ERROR, "[%s] CLIENT Session 이 아닌 세션 ID\n", __FUNCTION__);

		__debugbreak();
		return;
	}

	clientSession->DecrementIO();

	bool bResult = clientSession->OnSendCompleted(bytesTransferred);
}

void IOCPClient::HandleSendCancelled(OverlappedEx* overlappedEx, ISession* session)
{
	if (!session)
		return;

	if (!overlappedEx)
		return;

	// 걸어 두었던 WSASend 에 대한 IO 감소
	session->DecrementIO();

	printf_s("[IOCPClient] :: [Session ID : %d] Send Cancelled.\n", session->GetSessionID());
}

void IOCPClient::HandleSessionDisconnected(OverlappedEx* overlappedEx, ISession* session, DWORD bytesTransferred)
{
	// 세션의 정상 종료 시퀀스

	if (session == nullptr || overlappedEx == nullptr)
	{
		__debugbreak();
		return;
	}

	if (overlappedEx->operation == IO_OPERATION::RECV)
	{
		OnDisconnectRequest(session);
	}
	else if (overlappedEx->operation == IO_OPERATION::SEND)
	{
		__debugbreak();

	}
	else
	{
		__debugbreak();
	}
}

bool IOCPClient::CreateConnectSocket()
{
	m_clientSocket = ::WSASocket(AF_INET, SOCK_STREAM, IPPROTO_TCP, NULL, 0, WSA_FLAG_OVERLAPPED);

	if (m_clientSocket == INVALID_SOCKET)
	{
		//Log::log(LogLevel::LOG_ERROR, "[%s] WSASocket Connect socket failed\n", __FUNCTION__);
		return false;
	}

	//Log::log(LogLevel::LOG_INFO, "[%s] WSASocket Connect socket success\n", __FUNCTION__);

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
		//Log::log(LogLevel::LOG_ERROR, "[%s] bind failed - ErroCode : %d\n", __FUNCTION__, WSAGetLastError());

		return false;
	}

	//Log::log(LogLevel::LOG_INFO, "[%s] bind success\n", __FUNCTION__);

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
		//Log::log(LogLevel::LOG_ERROR, "[%s] WSAIoctl Get ConnectEx Pointer Failed - ErroCode : %d\n", __FUNCTION__, WSAGetLastError());

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
		//Log::log(LogLevel::LOG_ERROR, "[%s] Failed to acquire session\n", __FUNCTION__);
		__debugbreak();
		return false;
	}

	if (m_session->GetClientSessionState() != ClientSessionState::CONNECT_READY)
	{
		//Log::log(LogLevel::LOG_ERROR, "[%s] Session is not Ready\n", __FUNCTION__);
		__debugbreak();
		return false;
	}

	if (!PostConnect(m_session))
	{
		//Log::log(LogLevel::LOG_ERROR, "[%s] Failed to post ConnectEx\n", __FUNCTION__);
		return false;
	}

	return true;
}

bool IOCPClient::PostConnect(ISession* session)
{
	ClientSession* clientSession = dynamic_cast<ClientSession*>(session);

	if (!clientSession)
	{
		Logger::Log(LogLevel::LOG_ERROR, "[%s] ConnectEx 시작하려고 세션을 캐스팅 했는데 nullptr", __FUNCTION__);

		return false;
	}

	SOCKADDR_IN serveraddr = { 0 };
	serveraddr.sin_family = AF_INET;
	serveraddr.sin_port = htons(m_serverPort);
	int nResult = ::inet_pton(AF_INET, m_serverIPAddress, &serveraddr.sin_addr);

	if (nResult == 0)
	{
		// 유효한 IPv4 점선 10진수 문자열 또는 유효한 IPv6 주소 문자열이 아닌 문자열을 가리키는 경우
		__debugbreak();
	}
	else if (nResult == -1)
	{
		int nError = ::WSAGetLastError();
		__debugbreak();
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
		Logger::Log(LogLevel::LOG_ERROR, "[%s] Client Session is nullptr", __FUNCTION__);

		return;
	}

	if (clientSession->GetClientSocket() != INVALID_SOCKET)
	{
		// Connect, Recv, Send IO 를 모두 취소하고
		if (!clientSession->CancelPendingIO())
		{
			__debugbreak();
		}

		// 취소가 완료되기를 기다린다.
		if (!clientSession->WaitForIOCancelComplete(10'000))
		{
			__debugbreak();
		}

		// 모든 IO 취소가 성공적으로 수행되었으면
		// 소켓을 닫는다.
		DestroyConnectSocket();
	}

	// 소켓을 닫은 이후 세션에 대한 상태 및 정리를 수행
	if (!clientSession->OnDisconnect())
	{
		__debugbreak();
	}
}

PacketHandlerTable* IOCPClient::GetPacketHandlerTable() const
{
	return m_packetHandlerTable;
}

SlabMemoryPool* IOCPClient::GetJobMemoryPool() const
{
	return m_jobMemoryPool;
}

SlabMemoryPool* IOCPClient::GetPacketMemoryPool() const
{
	return m_packetMemoryPool;
}

SlabMemoryPool* IOCPClient::GetGeneralMemoryPool() const
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
			Logger::Log(LogLevel::LOG_ERROR, "[%s][ClientSession : %d] protocol version mismatch (server=%u, client=%u)", __FUNCTION__, session->GetSessionID(), response->protocolVersion, IOCP_ENGINE_PROTOCOL_VERSION);
			return false;
		}

		if (authResult != SYSTEM_AUTH_RESULT::SUCCESS)
		{
			Logger::Log(LogLevel::LOG_WARNING, "[%s][ClientSession : %d] auth rejected by server (result=%u)", __FUNCTION__, session->GetSessionID(), response->authResult);
			return false;
		}

		session->SetClientSessionState(ClientSessionState::ESTABLISHED);
		Logger::Log(LogLevel::LOG_INFO, "[%s][ClientSession : %d] session established", __FUNCTION__, session->GetSessionID());
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
		Logger::Log(LogLevel::LOG_WARNING, "[%s][ClientSession : %d] unhandled system packet id: %u", __FUNCTION__, session->GetSessionID(), packetId);
		return false;
	}
}
