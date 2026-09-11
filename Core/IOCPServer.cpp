#include "IOCPServer.h"
#include "HeartbeatThread.h"

#include "../Diagnostics/EngineAssert.h"

#include <WinSock2.h>
#include <MSWSock.h> // for AcceptEX
#include <Ws2tcpip.h> // for inet_ntop

#include <stdio.h>
#include <stdlib.h>

#include "IOCPCore.h"

#include "../Job/Job.h"
#include "../Scheduler/ReadySessionQueue.h"
#include "../Scheduler/ReadySessionScheduler.h"
#include "../HandlerTable/PacketHandlerTable.h"

#include "../Memory/EngineMemoryPool.h"
#include "../Memory/EngineMemoryPoolHelper.h"

#include "../Network/SocketOption.h"

#include "../Buffer/RecvPacketBuffer.h"
#include "../Buffer/SendPacketEntry.h"
#include "../Buffer/PreDefine.h"
#include "../Protocol/PacketID.h"

#include "../Session/ISession.h"
#include "../Session/SessionManager.h"
#include "../Session/ClientSession.h"
#include "../Session/AcceptSession.h"
#include "../Session/SessionJobQueue.h"

#include "../../Core/Util/Logger.h"

#pragma comment(lib, "ws2_32.lib") // for WinSock2
#pragma comment(lib, "mswsock.lib") // for AcceptEX / ConnectEx

using namespace Core::Util;

namespace
{
	// 연결이 죽어서 난 I/O 실패인가.
	//
	// ERROR_OPERATION_ABORTED 는 여기 없다. 그건 우리가 CancelIoEx 로 건
	// 취소이고, 취소를 건 쪽이 이미 세션을 정리하는 중이다. 둘을 같이
	// 묶어 두었던 것이 이 파일의 오래된 결함이었다 — 연결이 죽은 경우까지
	// "취소" 로 처리해서 IO 카운트만 내리고 세션은 살려 두었다.
	bool IsConnectionDeadError(int errorCode)
	{
		switch (errorCode)
		{
		case WSAECONNRESET:         // 상대가 강제 종료 (RST)
		case WSAECONNABORTED:       // 연결 중단
		case WSAENOTCONN:           // 이미 끊김
		case WSAESHUTDOWN:          // 송수신 불가
		case ERROR_NETNAME_DELETED: // 네트워크 이름 삭제 = 연결 끊김
			return true;
		default:
			return false;
		}
	}
}

IOCPServer::IOCPServer()
{
}

IOCPServer::~IOCPServer()
{
	StopServer();
}

bool IOCPServer::StartServer(const char* ipAddress, const uint16_t port, const uint32_t maxConnectionCount, const SessionBufferConfig& bufferConfig, const ConnectionPolicyConfig& policyConfig)
{
	// 설정 오류는 아무것도 잡기 전에 걸러낸다. 세션 생성 단계까지 끌고 가면
	// 세션을 하나도 못 잡는 서버가 정상 기동한 것처럼 보인다.
	if (!bufferConfig.IsValid())
	{
		LOGE("invalid session buffer config : recv %u / ring %u, send %u, queue %u",
			bufferConfig.maxRecvPacketSize, bufferConfig.recvRingSize,
			bufferConfig.maxSendPacketSize, bufferConfig.sendQueueDepth);
		return false;
	}

	// serviceCapacity 가 세션 풀보다 크면 아무 효과가 없다. 그런 설정은
	// "제한을 걸었다" 고 믿게 만들므로 기동을 실패시킨다.
	if (policyConfig.serviceCapacity > maxConnectionCount)
	{
		LOGE("service capacity %u exceeds the session pool capacity %u",
			policyConfig.serviceCapacity, maxConnectionCount);
		return false;
	}

	m_connectionPolicy = policyConfig;

	LOGI("connection policy : service capacity %u (0 = pool capacity %u), max per address %u (0 = unlimited)",
		m_connectionPolicy.serviceCapacity, maxConnectionCount, m_connectionPolicy.maxConnectionsPerAddress);

	SetIOCPThreadCount(::GetMaximumProcessorCount(ALL_PROCESSOR_GROUPS));

	if (!IOCPCore::Start())
	{
		return false;
	}

	if (!CreateListenSocket(ipAddress, port))
	{
		return false;
	}

	if (!BindServerSocket(m_serverSocket, ipAddress, port))
	{
		return false;
	}

	if (!ListenServerSocket(m_serverSocket))
	{
		return false;
	}

	if (!InitializeGUIDAcceptEx(m_serverSocket))
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
		{MEMORY_SIZE_1K, 1024},
		{MEMORY_SIZE_2K, 1024},
		{MEMORY_SIZE_4K, 1024},
		{MEMORY_SIZE_8K, 512},
		{MEMORY_SIZE_16K, 512},
		{MEMORY_SIZE_32K, 512},
	};

	m_packetMemoryPool = new EngineMemoryPool;
	if (!m_packetMemoryPool)
		return false;

	// 바로 위 job 풀은 반환값을 검사하는데 여기 둘은 버리고 있었다.
	// 실패하면 bin 이 하나도 없는 풀이 그대로 살아남아, 첫 패킷 할당에서야
	// 정체 모를 실패로 드러난다. 기동 시점에 끊는 편이 낫다.
	if (!m_packetMemoryPool->Initialize(configsPacket, _countof(configsPacket)))
	{
		LOGE("failed to initialize the packet memory pool");
		return false;
	}

	EngineMemoryPool::SlabConfig configsImageBuffer[] = {
		{MEMORY_SIZE_1MB, 1},
		//{MEMORY_SIZE_4MB, 1},
		//{MEMORY_SIZE_8MB, 1}
	};

	m_generalMemoryPool = new EngineMemoryPool;
	if (!m_generalMemoryPool)
		return false;

	if (!m_generalMemoryPool->Initialize(configsImageBuffer, _countof(configsImageBuffer)))
	{
		LOGE("failed to initialize the general memory pool");
		return false;
	}

	// 송신 큐 엔트리 풀. 빈은 하나면 된다 — 담는 것이 한 종류뿐이다.
	EngineMemoryPool::SlabConfig configsSendQueue[] = {
		{sizeof(SendPacketEntry), SEND_QUEUE_ENTRY_COUNT},
	};

	m_sendQueueMemoryPool = new EngineMemoryPool;
	if (!m_sendQueueMemoryPool)
		return false;

	if (!m_sendQueueMemoryPool->Initialize(configsSendQueue, _countof(configsSendQueue)))
	{
		LOGE("failed to initialize the send queue memory pool");
		return false;
	}

	m_readySessionQueue = new ReadySessionQueue;
	if (!m_readySessionQueue)
		return false;

	const uint32_t sessionQueueBufferCount = maxConnectionCount/* * 2*/;
	if (!m_readySessionQueue->Initialize(sessionQueueBufferCount))
	{
		LOGE("failed to initialize the ready session queue");
		return false;
	}

	m_handlerContext.jobMemoryPool = GetJobMemoryPool();
	m_handlerContext.packetMemoryPool = GetPacketMemoryPool();
	m_handlerContext.generalMemoryPool = GetGeneralMemoryPool();
	m_handlerContext.serviceContext = GetServiceContext();

	m_packetHandlerTable = new PacketHandlerTable();
	if (!m_packetHandlerTable)
		return false;

	m_readySessionScheduler = new ReadySessionScheduler;
	if (!m_readySessionScheduler)
		return false;

	uint32_t sessionProcessorThreadCount = ::GetMaximumProcessorCount(ALL_PROCESSOR_GROUPS);
	if (sessionProcessorThreadCount == 0)
		sessionProcessorThreadCount = ::GetActiveProcessorCount(ALL_PROCESSOR_GROUPS);
	if (sessionProcessorThreadCount == 0)
		sessionProcessorThreadCount = 1;
	if (!m_readySessionScheduler->Initialize(sessionProcessorThreadCount, m_readySessionQueue, m_jobMemoryPool, m_packetMemoryPool, m_generalMemoryPool))
	{
		LOGE("failed to initialize the ready session scheduler");
		return false;
	}

	m_sessionManager = new SessionManager;
	if (!m_sessionManager)
		return false;

	constexpr uint32_t PreferredAcceptSlotCount = 16;

	uint32_t acceptSlotCount = maxConnectionCount < PreferredAcceptSlotCount
		? maxConnectionCount
		: PreferredAcceptSlotCount;

	if (acceptSlotCount == 0)
		acceptSlotCount = 1;

	m_desiredAcceptCount = acceptSlotCount;

	LOGI("accept slots %u (max connections %u)", acceptSlotCount, maxConnectionCount);

	if (!m_sessionManager->Initialize(acceptSlotCount, maxConnectionCount, m_sendQueueMemoryPool, m_jobMemoryPool, m_packetMemoryPool, m_generalMemoryPool, IOCPCore::CloseSocketHandle, bufferConfig))
		return false;

	// 세션 종료 통지를 세션 풀에 맡긴다.
	//
	// 이 서버가 직접 부르지 않는다. 반납 경로가 여럿이라(소켓 오류 /
	// 좀비 정리 / 파싱 실패 / 접속 시퀀스 실패 / 서버 종료) 부르는 자리를
	// 여기저기 두면 하나씩 빠진다. 실제로 그랬다 — 정상 종료 한 경로에만
	// 있었다. 반납이 실제로 일어나는 곳은 풀 하나뿐이므로 거기서 부른다.
	m_sessionManager->SetSessionDisconnectNotifyFunc(&IOCPServer::OnSessionDisconnectNotify, this);

	if (!PrepareAccept())
	{
		return false;
	}

	constexpr uint64_t HeartbeatCheckInterval_ms = 5'000;
	constexpr uint64_t HeartbeatTimeout_ms = 15'000;
	// 걸기가 실패해 비어 버린 accept 슬롯을 다시 post 하는 일을 이 스레드의
	// 주기에 얹는다. 실패한 슬롯은 스스로 복구되지 않으므로 (다음 PostAccept 를
	// 부를 완료 통지가 애초에 그 실패한 걸기의 것이다) 누군가 다시 걸어 주어야
	// 한다. 자세한 이유는 AcceptRepostFunc 선언부 주석에 있다.
	m_heartbeatThread = new HeartbeatThread(m_sessionManager, HeartbeatCheckInterval_ms, HeartbeatTimeout_ms,
		[](void* context) { static_cast<IOCPServer*>(context)->RefillAcceptSlots(); }, this);
	if (!m_heartbeatThread)
	{
		return false;
	}

	if (!m_heartbeatThread->Start())
	{
		delete m_heartbeatThread;
		m_heartbeatThread = nullptr;
		return false;
	}

	return true;
}

void IOCPServer::StopServer()
{
	LOGI("server shutdown requested");

	// 서버가 닫히기 전에 AcceptIO 가 취소되기 전에 그 찰나에 받아진
	// Accept IO 에 대한 예외 처리를 위해 플래그 추가.
	::InterlockedExchange(&m_serverShutdownRequested, TRUE);

	if (m_heartbeatThread)
	{
		m_heartbeatThread->Stop();
		delete m_heartbeatThread;
		m_heartbeatThread = nullptr;
	}

	if (m_sessionManager)
	{
		LOGI("shutdown 1/6 : cancelling all AcceptEx IO");
		m_sessionManager->RequestAllAcceptIOCancel();

		LOGI("shutdown 2/6 : waiting for AcceptEx cancellation");
		m_sessionManager->WaitForAllAcceptIOCancelComplete(10'000);
	}

	if (m_sessionManager)
	{
		LOGI("shutdown 3/6 : cancelling all recv/send IO");
		m_sessionManager->RequestAllRecvSendIOCancel();

		LOGI("shutdown 4/6 : waiting for recv/send cancellation");
		m_sessionManager->WaitForAllRecvSendIOCancelComplete(10'000);

		LOGI("shutdown 5/6 : closing all client sockets");
		m_sessionManager->DisconnectAllSessions();
	}

	// 서버 소켓의 모든 Accept I/O 를 취소시킨다.
	// AcceptEx 를 사용하므로 모든 세션이 이미 Accept GQCS 에 등록되어 있다.
	LOGI("shutdown 6/6 : cancelling listen socket IO");
	if (!CancelAllAcceptIO())
	{
		ENGINE_VIOLATION("failed to cancel the listen socket IO, accept completions may be left pending");
	}

	//if (m_sessionManager)
	//{
	//	LOGI("shutdown 2/6 : waiting for AcceptEx cancellation");
	//	m_sessionManager->WaitForAllAcceptIOCancelComplete(10'000);
	//}


	// 서버 리슨 소켓을 닫아서 새 클라이언트 연결이 들어오는걸 먼저 막아야 한다.
	LOGI("closing the listen socket");
	DestroyListenSocket();

	FinalizeGUIDAcceptEx();

	// 연결 되어 있는 세션을 강제로 Disconnect 하고 클라이언트 소켓을 닫는다.
	LOGI("forcing the remaining sessions to disconnect");

	LOGI("shutting down the IOCP core");
	IOCPCore::Stop();

	if (m_sessionManager)
	{
		delete m_sessionManager;
		m_sessionManager = nullptr;
	}

	if (m_readySessionScheduler)
	{
		delete m_readySessionScheduler;
		m_readySessionScheduler = nullptr;
	}

	if (m_readySessionQueue)
	{
		delete m_readySessionQueue;
		m_readySessionQueue = nullptr;
	}

	if (m_packetHandlerTable)
	{
		delete m_packetHandlerTable;
		m_packetHandlerTable = nullptr;
	}

	// 풀 지표를 남긴다.
	// peak 은 초기 blockCount 산정 근거이고, grow 가 0 이 아니면 초기값이 부족했다는 뜻이며
	// OUTSTANDING 이 0 이 아니면 누수다. 개별 할당 로그 없이 이걸로 판단한다.
	//
	// sendQueue 는 세션이 전부 내려간 뒤에 찍어야 한다 (세션 매니저를 이미
	// 지웠으므로 그 조건은 만족한다). 여기서 grow 가 0 이 아니면 초기
	// SEND_QUEUE_ENTRY_COUNT 로는 버스트를 못 받았다는 뜻이다.
	if (m_jobMemoryPool)       m_jobMemoryPool->LogStats("job");
	if (m_packetMemoryPool)    m_packetMemoryPool->LogStats("packet");
	if (m_generalMemoryPool)   m_generalMemoryPool->LogStats("general");
	if (m_sendQueueMemoryPool) m_sendQueueMemoryPool->LogStats("sendQueue");

	if (m_sendQueueMemoryPool)
	{
		delete m_sendQueueMemoryPool;
		m_sendQueueMemoryPool = nullptr;
	}

	if (m_jobMemoryPool)
	{
		delete m_jobMemoryPool;
		m_jobMemoryPool = nullptr;
	}

	if (m_packetMemoryPool)
	{
		delete m_packetMemoryPool;
		m_packetMemoryPool = nullptr;
	}

	if (m_generalMemoryPool)
	{
		delete m_generalMemoryPool;
		m_generalMemoryPool = nullptr;
	}

	Logger::Flush();
}

void IOCPServer::HandleCompletion(
	ULONG_PTR completionKey,
	LPOVERLAPPED overlapped,
	DWORD bytesTransferred,
	BOOL completionStatus)
{
	OverlappedEx* overlappedEx = reinterpret_cast<OverlappedEx*>(overlapped);

	// GQCS 자체가 실패하면 overlapped 가 nullptr 로 온다.
	// 종료 중 IOCP 핸들이 닫히는 경우가 대표적이므로 위반으로 다루지 않는다.
	if (overlappedEx == nullptr)
	{
		LOGW("completion arrived with no overlapped (status %d, error %lu). the IOCP handle was most likely closed",
			completionStatus, ::GetLastError());
		return;
	}

	switch (overlappedEx->operation)
	{
	case IO_OPERATION::ACCEPT:
		if (!completionStatus)
		{
			HandleSocketError(overlappedEx, nullptr, ::WSAGetLastError(), IO_OPERATION::ACCEPT);
			return;
		}

		HandleAccept(overlappedEx->sessionId, bytesTransferred);
		return;

	case IO_OPERATION::RECV:
	case IO_OPERATION::SEND:
	{
		// 완료 키에 넣은 것이 ClientSession* 다 (HandleAccept 의
		// RegisterSocketToIOCP 참고). 예전에는 ISession* 로 되받아서
		// 계층을 가로지르는 reinterpret_cast 를 했고, 단일 상속이라
		// 주소가 우연히 같아서 동작했을 뿐이다. 넣은 타입 그대로 받는다.
		ClientSession* session = reinterpret_cast<ClientSession*>(completionKey);
		ENGINE_CHECK_RETVOID(session != nullptr,
			"io %d completion carried no session in the completion key",
			static_cast<int>(overlappedEx->operation));

		if (!completionStatus)
		{
			HandleSocketError(overlappedEx, session, ::WSAGetLastError(), overlappedEx->operation);
			return;
		}

		if (overlappedEx->operation == IO_OPERATION::RECV)
			HandleRecv(overlappedEx, session, bytesTransferred);
		else
			HandleSend(overlappedEx, session, bytesTransferred);

		return;
	}

	default:
		ENGINE_VIOLATION("completion arrived with an unknown io operation %d",
			static_cast<int>(overlappedEx->operation));
		return;
	}
}

void IOCPServer::HandleSocketError(OverlappedEx* overlappedEx, ClientSession* session, int errorCode, IO_OPERATION ioOperation)
{
	// 공용으로 처리 되어야 하는 예외 처리

	if (ioOperation == IO_OPERATION::ACCEPT)
	{
		LOGW("accept io %d failed (error %d)", (int)ioOperation, errorCode);

		if (errorCode == ERROR_OPERATION_ABORTED)
		{
			// CancelIoEx 로 인한 I/O 를 강제로 취소한 경우
			// 또는 서버 리슨 소켓을 닫은 경우
			// 이전에 걸어 두었던 Accept 에 대해 취소된 IO 작업 완료 알림 수신

			HandleAcceptIOCancelled(overlappedEx->sessionId);

			return;
		}

		// 취소가 아닌 accept 실패다. 해당 accept 세션의 IO 카운트를 정리해야
		// 종료 시 취소 대기가 풀린다.
		ENGINE_VIOLATION("accept failed with error %d (not a cancellation), releasing the accept slot", errorCode);
		HandleAcceptIOCancelled(overlappedEx->sessionId);

		return;
	}
	else if (ioOperation == IO_OPERATION::RECV)
	{
		if (session == nullptr)
		{
			ENGINE_VIOLATION("socket error on io %d but no session was supplied (error %d)", static_cast<int>(ioOperation), errorCode);
			return;
		}

		LOGW("session %u recv io failed (error %d)", session->GetSessionID(), errorCode);

		if (errorCode == ERROR_OPERATION_ABORTED)
		{
			// 우리가 CancelIoEx 로 취소한 것이다. 취소를 건 쪽이 이미 이
			// 세션을 정리하고 있으므로 카운트만 내려놓는다.
			HandleRecvCancelled(overlappedEx, session);
			return;
		}

		if (!IsConnectionDeadError(errorCode))
		{
			// 분류되지 않은 에러 코드. 이 스트림을 계속 믿을 근거가 없으므로
			// 아래에서 연결이 죽은 것과 같이 다룬다.
			ENGINE_VIOLATION("session %u unhandled recv error %d, treating the connection as dead",
				session->GetSessionID(), errorCode);
		}

		// 연결이 죽었다. 취소와는 다르다 — 아무도 이 세션을 정리하고 있지 않다.
		//
		// 예전에는 여기도 HandleRecvCancelled 로만 보냈다. 그 함수는 IO
		// 카운트만 내린다. 그래서 이 세션은 걸린 수신 없이 IN_USE 로 남아
		// 슬롯을 붙들었고, 하트비트가 타임아웃으로 걷어 갈 때까지 살아 있었다.
		//
		// 실측(Phase 17 churn): 죽은 세션 하나가 4.5초를 붙들었다. 그것도
		// 타임아웃이 아니라 하트비트 '송신' 이 마침 실패해서 풀린 것이라,
		// 회수가 운에 기대고 있었다. 피어가 반만 닫았으면 타임아웃까지 갔다.
		//
		// 반납을 먼저 예약하고, 카운트는 HandleRecvCancelled 가 마지막에 내린다.
		// 그 순서여야 우리 몫을 들고 있는 동안 반납이 예약 상태로 머문다.
		m_sessionManager->ReleaseClientSession(session);

		HandleRecvCancelled(overlappedEx, session);

		return;
	}
	else if (ioOperation == IO_OPERATION::SEND)
	{
		if (session == nullptr)
		{
			ENGINE_VIOLATION("socket error on io %d but no session was supplied (error %d)", static_cast<int>(ioOperation), errorCode);
			return;
		}

		LOGW("session %u send io failed (error %d)", session->GetSessionID(), errorCode);

		if (errorCode == ERROR_OPERATION_ABORTED)
		{
			// 우리가 건 취소다. 정리는 취소를 건 쪽이 한다.
			HandleSendCancelled(overlappedEx, session);
			return;
		}

		if (!IsConnectionDeadError(errorCode))
		{
			ENGINE_VIOLATION("session %u unhandled send error %d, treating the connection as dead",
				session->GetSessionID(), errorCode);
		}

		// 송신이 죽은 연결도 마찬가지다. 수신 쪽 주석 참고.
		m_sessionManager->ReleaseClientSession(session);

		HandleSendCancelled(overlappedEx, session);

		return;
	}
	else
	{
		ENGINE_VIOLATION("socket error reported for an unknown io operation %d (error %d)", static_cast<int>(ioOperation), errorCode);
		return;
	}
}

void IOCPServer::HandleAccept(uint32_t sessionId, DWORD bytesTransferred)
{
	LOGT("accept completed on accept session %u", sessionId);

	AcceptSession* acceptSession = m_sessionManager->GetAcceptSession(sessionId);

	if (!acceptSession)
	{
		LOGE("accept session cast failed : the session is not an AcceptSession");

		ENGINE_BREAK_IF_DEBUGGER();
		return;
	}

	acceptSession->OnAccept();

	// 완료된 AcceptEx 는 더 이상 걸려 있지 않다. 아래에서 PostAccept 가
	// 성공하면 다시 올라간다.
	//
	// IO 카운트(DecrementIO)와 달리 이건 순수한 계량이라 여기서 먼저 내려도
	// 된다. 고갈 경보는 다시 걸기를 시도한 뒤에 확인하므로 이 사이의 일시적
	// 감소를 고갈로 오해하지 않는다.
	::InterlockedDecrement(&m_postedAcceptCount);

	if (::InterlockedCompareExchange(&m_serverShutdownRequested, 0, 0) == TRUE)
	{
		IOCPCore::CloseSocketHandle(acceptSession->DetachSocket());

		if (!acceptSession->OnDisconnect())
		{
			ENGINE_VIOLATION("accept session %u OnDisconnect reported failure", acceptSession->GetSessionID());
		}

		acceptSession->DecrementIO();
		acceptSession->ResetSession();

		// 종료 중이라 다시 걸지 않는다. 슬롯을 놓고 나간다.
		acceptSession->ReleaseSlot();

		return;
	}

	// 원격 주소를 먼저 읽어 둔다. 예전에는 세션을 임대한 뒤에 읽었는데,
	// 주소당 접속 수 제한은 세션을 잡기 전에 판단해야 한다. 세션을 먼저
	// 잡으면 거절할 접속이 잠깐이라도 슬롯을 차지한다.
	char strIPAddress[INET_ADDRSTRLEN] = { 0, };
	uint16_t remotePort = 0;

	if (acceptSession->GetAcceptBuffer() && m_getAcceptExSockAddrs)
	{
		sockaddr_in* localAddr = nullptr;
		sockaddr_in* remoteAddr = nullptr;
		int localLen = 0;
		int remoteLen = 0;
		const int addrSize = sizeof(sockaddr_in) + 16;

		m_getAcceptExSockAddrs(
			acceptSession->GetAcceptBuffer(),
			0,
			addrSize,   // local address length
			addrSize,   // remote address length
			(sockaddr**)&localAddr, &localLen,
			(sockaddr**)&remoteAddr, &remoteLen
		);

		if (remoteAddr != nullptr)
		{
			if (::inet_ntop(AF_INET, &remoteAddr->sin_addr, strIPAddress, sizeof(strIPAddress)))
			{
				remotePort = ::ntohs(remoteAddr->sin_port);

				LOGT("accepted a connection from %s:%u", strIPAddress, remotePort);
			}
		}
	}

	// 주소당 동시 접속 제한. 한 주소가 세션 풀을 통째로 채우는 것을 막는다.
	// 세션을 잡기 전에 걸러야 의미가 있다.
	if (m_connectionPolicy.maxConnectionsPerAddress > 0 && strIPAddress[0] != '\0')
	{
		const uint32_t existing = m_sessionManager->CountClientSessionsFromAddress(strIPAddress);

		if (existing >= m_connectionPolicy.maxConnectionsPerAddress)
		{
			LOGW("rejected a connection from %s : %u connections already (limit %u)",
				strIPAddress, existing, m_connectionPolicy.maxConnectionsPerAddress);

			IOCPCore::CloseSocketHandle(acceptSession->DetachSocket());

			if (!acceptSession->OnDisconnect())
			{
				ENGINE_VIOLATION("accept session %u OnDisconnect reported failure", acceptSession->GetSessionID());
			}

			acceptSession->ResetSession();

			const bool nextAcceptPosted = PostAccept(acceptSession);

			if (!nextAcceptPosted)
			{
				// 슬롯이 실제로 비었다. 놓아야 주기 점검이 다시 채울 수 있다.
				acceptSession->ReleaseSlot();
			}

			acceptSession->DecrementIO();

			// 걸려 있는 AcceptEx 가 하나도 없으면 새 접속을 못 받는 상태다.
			ReportAcceptStarvationIfNeeded();

			if (!nextAcceptPosted)
			{
				LOGE("failed to post the next AcceptEx after rejecting a connection (per-address limit)");
			}

			return;
		}
	}

	ClientSession* clientSession = nullptr;

	if (!m_sessionManager->IsClientSessionFull())
	{
		// Client Session Pool 이 여유 있는 경우
		//
		// 예전에는 AcquireClientSession 이 ISession* 를 돌려주어 여기서
		// dynamic_cast 로 되돌렸다. 풀에는 ClientSession 만 들어가므로
		// 그 RTTI 조회는 절대 실패할 수 없는 검사였고, 실패를 "풀 고갈" 로
		// 착각하게 만드는 분기까지 달고 있었다. 이제 임대가 구체 타입을
		// 돌려주므로 아래 널 검사는 순수하게 고갈만 뜻한다.
		clientSession = m_sessionManager->AcquireClientSession();

		if (!clientSession)
		{
			// Client Session 획득에 실패한 경우
			// Full 체크 할 때에는 여유 있었는데
			// 그 사이에 다른 스레드가 클라이언트 세션을 가져갔는데
			// 마침 세션 프리리스트에 대기 중인 세션이 없는 경우
			// 해당 연결을 강제 종료 처리 후 Accept 를 걸어 준다.

			IOCPCore::CloseSocketHandle(acceptSession->DetachSocket());

			if (!acceptSession->OnDisconnect())
			{
				ENGINE_VIOLATION("accept session %u OnDisconnect reported failure", acceptSession->GetSessionID());
			}

			acceptSession->ResetSession();

			const bool nextAcceptPosted = PostAccept(acceptSession);

			if (!nextAcceptPosted)
			{
				// 슬롯이 실제로 비었다. 놓아야 주기 점검이 다시 채울 수 있다.
				acceptSession->ReleaseSlot();
			}

			// 완료된 AcceptEx 1건에 대한 우리 몫의 카운트를 내린다.
			// 다음 accept 를 건 뒤에 내리는 이유는 recv/send 와 같다 — 먼저 내리면
			// 그 사이 카운트가 0 이 되어 종료 시의 취소 대기가 통과한다.
			acceptSession->DecrementIO();

			// 걸려 있는 AcceptEx 가 하나도 없으면 새 접속을 못 받는 상태다.
			ReportAcceptStarvationIfNeeded();

			if (!nextAcceptPosted)
			{
				LOGE("failed to post the next AcceptEx after the client session pool handed out nothing");

				return;
			}
		}
		else
		{
			// Client Session 획득에 성공한 경우
			// socket 을 붙이고 IOCP 에 등록 후 PostRecv 하러 간다.

			// 주소는 이 함수 앞부분에서 이미 읽어 두었다. (주소당 제한을
			// 세션 임대 전에 판단해야 해서 앞으로 옮겼다)

			SOCKET acceptedSocket = acceptSession->DetachSocket();

			if (clientSession->GetClientSocket() != INVALID_SOCKET || clientSession->GetServerSessionState() != ServerSessionState::CONNECT_READY)
			{
				// 풀에서 막 임대한 세션이 깨끗하지 않다. 사용 중인 세션이 재배포된 것이다.
				ENGINE_VIOLATION("session %u was handed out but is not clean (socket %d, state %d)",
					clientSession->GetSessionID(), static_cast<int>(clientSession->GetClientSocket()), static_cast<int>(clientSession->GetServerSessionState()));
			}

			LOGI("socket %d attached to session %u (via accept session %u)", (int)acceptedSocket, clientSession->GetSessionID(), acceptSession->GetSessionID());

			clientSession->AttachSocket(acceptedSocket);
			clientSession->SetRemoteAddress(strIPAddress, remotePort);

			// Session 과 1:1 대응하는 User 를 초기화 한다.
			// Session 은 네트워크 담당, User 는 서비스 로직을 담당한다.
			//User* pUser = m_userManager->GetUser(clientSession->GetSessionID());
			//pUser->ResetUser();
			//if (!pUser->Initialize(clientSession, clientSession->GetSessionID()))
			//{
			//	__debugbreak();
			//}
			//clientSession->SetUser(pUser);

			const bool nextAcceptPosted = PostAccept(acceptSession);

			if (!nextAcceptPosted)
			{
				// 슬롯이 실제로 비었다. 놓아야 주기 점검이 다시 채울 수 있다.
				acceptSession->ReleaseSlot();
			}

			// 완료된 AcceptEx 1건에 대한 우리 몫의 카운트를 내린다.
			// 이게 빠져 있어서 수락 1건마다 카운트가 +1 로 새고, 종료 시
			// WaitForIOCancelComplete 가 0 을 못 봐서 10초씩 태웠다.
			acceptSession->DecrementIO();

			// 걸려 있는 AcceptEx 가 하나도 없으면 새 접속을 못 받는 상태다.
			ReportAcceptStarvationIfNeeded();

			if (!nextAcceptPosted)
			{
				LOGE("failed to post the next AcceptEx after attaching the accepted socket");

				return;
			}
		}
	}
	else
	{
		// Client Session Pool 이 여유 없는 경우
		// 해당 연결을 강제 종료 처리 후 Accept 를 걸어 준다.

		LOGW("accept session %u rejected a connection : the client session pool is full", acceptSession->GetSessionID());

		IOCPCore::CloseSocketHandle(acceptSession->DetachSocket());

		if (!acceptSession->OnDisconnect())
		{
			ENGINE_VIOLATION("accept session %u OnDisconnect reported failure", acceptSession->GetSessionID());
		}

		acceptSession->ResetSession();

		const bool nextAcceptPosted = PostAccept(acceptSession);

		if (!nextAcceptPosted)
		{
			// 슬롯이 실제로 비었다. 놓아야 주기 점검이 다시 채울 수 있다.
			acceptSession->ReleaseSlot();
		}

		// 완료된 AcceptEx 1건에 대한 우리 몫의 카운트를 내린다.
		acceptSession->DecrementIO();

		// 걸려 있는 AcceptEx 가 하나도 없으면 새 접속을 못 받는 상태다.
		ReportAcceptStarvationIfNeeded();

		if (!nextAcceptPosted)
		{
			LOGE("failed to post the next AcceptEx after rejecting a connection (pool full)");
		}

		return;
	}

	ENGINE_CHECK_RETVOID(clientSession != nullptr, "reached the connect sequence without a client session");

	// 접속 시퀀스가 도는 동안 이 세션을 붙잡아 둔다.
	//
	// 시퀀스 안에서 세션이 반납될 수 있다. OnConnect 의 첫 PostReceive 가
	// 실패하면 HandleSocketError -> NotifyDisconnect -> ReleaseClientSession
	// 으로 이어지고, 지연 반납은 기다리지 않으므로 그 자리에서 반납이
	// 끝난다. 그러면 아래 남은 코드와 실패 처리의 ReleaseClientSession 은
	// 이미 프리 리스트에 올라간 세션을, 운이 나쁘면 그 사이 다른 접속이
	// 임대해 간 세션을 건드린다.
	//
	// 카운트를 하나 들고 있으면 그 반납은 예약 상태로 미뤄지고, 아래
	// 마지막 DecrementIO 가 0 으로 내리는 순간에 마무리된다.
	// 완료 핸들러들이 자기 몫을 끝까지 들고 있는 것과 같은 규칙이다.
	clientSession->IncrementIO();

	if (!RunAcceptedConnectSequence(clientSession))
	{
		m_sessionManager->ReleaseClientSession(clientSession);
	}

	// 이 함수의 마지막 줄이어야 한다. 여기서 반납이 마무리될 수 있으므로
	// 이후로 clientSession 을 만지면 안 된다.
	clientSession->DecrementIO();
}

bool IOCPServer::RunAcceptedConnectSequence(ClientSession* clientSession)
{
	LOGT("session %u running the server-side connect sequence", clientSession->GetSessionID());

	if (::InterlockedCompareExchange(&m_serverShutdownRequested, 0, 0) == TRUE)
	{
		// 이미 서버가 Shutdown 모드면 이 Accept 결과는 버린다.
		return false;
	}

	if (!SocketOption::SetAcceptContext(clientSession->GetClientSocket(), m_serverSocket))
	{
		LOGE("session %u SO_UPDATE_ACCEPT_CONTEXT failed, dropping the connection", clientSession->GetSessionID());
		return false;
	}

	if (!SocketOption::SetNoDelay(clientSession->GetClientSocket()))
	{
		LOGE("session %u TCP_NODELAY failed, dropping the connection", clientSession->GetSessionID());
		return false;
	}

	if (!SocketOption::SetKeepAliveEx(clientSession->GetClientSocket(), 10'000, 1'000))
	{
		LOGE("session %u keepalive setup failed, dropping the connection", clientSession->GetSessionID());
		return false;
	}
	//SocketOption::SetLinger(clientsocket, false); // 소켓 종료 즉시(선택)

	if (!IOCPCore::RegisterSocketToIOCP((ULONG_PTR)clientSession, clientSession->GetClientSocket()))
	{
		LOGE("session %u could not be associated with the IOCP, dropping the connection", clientSession->GetSessionID());
		return false;
	}

	if (!clientSession->OnConnect())
	{
		return false;
	}

	// 통지를 부르기 전에 표시한다. 부른 뒤에 표시하면 그 사이에 세션이
	// 반납될 경우(서비스가 OnClientConnect 안에서 바로 끊는 경우가 있다)
	// 표시가 서지 않아 종료 통지를 놓친다.
	clientSession->MarkServiceConnectNotified();

	OnClientConnect(clientSession);

	//Log::log(LogLevel::LOG_INFO, "[%s] New connection accepted. socket: %d", session->getclientsocket());

	return true;
}

void IOCPServer::HandleAcceptIOCancelled(uint32_t sessionId)
{
	LOGW("AcceptEx was cancelled on the listen socket");

	AcceptSession* acceptSession = m_sessionManager->GetAcceptSession(sessionId);

	if (!acceptSession)
	{
		LOGE("accept session cast failed : the session is not an AcceptSession");

		return;
	}

	acceptSession->SetAcceptSessionState(AcceptSessionState::ACCEPT_ABORTED);

	// 취소된 AcceptEx 는 더 이상 걸려 있지 않다.
	::InterlockedDecrement(&m_postedAcceptCount);
	acceptSession->DecrementIO();

	// 이 경로는 다시 걸지 않는다. 슬롯이 비었으므로 소유권을 놓는다.
	// 종료 중이면 주기 점검이 어차피 걸지 않고, 종료가 아닌 accept 실패라면
	// 이 슬롯은 주기 점검이 되살려야 한다.
	acceptSession->ReleaseSlot();
}

void IOCPServer::HandleRecv(OverlappedEx* overlappedEx, ClientSession* session, DWORD bytesTransferred)
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

		HandleSessionDisconnected(overlappedEx, session, bytesTransferred);

		return;
	}

	// 큐에는 ClientSession 만 들어가므로 RTTI 조회가 필요하지 않다.
	ClientSession* clientSession = static_cast<ClientSession*>(session);

	// DecrementIO 는 이 함수 끝에서 한다.
	//
	// 이전에는 여기 함수 앞에서 감소시켰다. 그러면 카운트가 0 이 된 뒤에도
	// 이 핸들러가 세션을 계속 사용하므로(파싱, 디스패치, PostReceive),
	// "카운트 0 = 아무도 세션을 만지지 않음" 이 성립하지 않았다.
	// 그 틈에 다른 스레드가 WaitForIOCancelComplete 를 통과해서 세션을
	// 리셋하고 풀로 반납하면, 이 핸들러는 이미 회수된 세션을 계속 만진다.
	//
	// 세션 반납이 필요한 경로도 즉시 부르지 않고 표시만 해 둔다.
	// ReleaseClientSession 은 카운트가 0 이 되기를 기다리므로,
	// 우리 몫을 내려놓기 전에 부르면 자기 자신을 기다리게 된다.
	bool releaseSessionAfterHandling = false;

	clientSession->UpdateLastRecvTick();

	RecvPacketBuffer& recvBuf = clientSession->GetReceiveBuffer();

	if (!recvBuf.CommitWrite(bytesTransferred))
	{
		ENGINE_VIOLATION("session %u recv ring commit failed : %lu bytes would overflow (stored %u / capacity %u)",
			clientSession->GetSessionID(), bytesTransferred,
			recvBuf.GetStoredSize(), recvBuf.GetCapacity());

		clientSession->DecrementIO();
		return;
	}

	// 패킷 처리 루프
	while (!recvBuf.IsEmpty())
	{
		uint32_t packetSize = 0;
		uint16_t packetId = 0;
		char* packetDataByMemoryPool = nullptr;

		const PacketReadResult readResult = recvBuf.ReadPacket(packetDataByMemoryPool, packetSize, packetId);

		if (readResult == PacketReadResult::NeedMoreData)
		{
			// 아직 다 안 왔다. 다음 수신 완료에서 이어서 파싱한다.
			break;
		}

		if (readResult != PacketReadResult::Ok)
		{
			// Invalid 는 피어가 규칙 밖의 크기를 적어 보낸 것이고,
			// OutOfMemory 는 패킷 풀이 고갈된 것이다. 둘 다 이 스트림을
			// 더 이상 신뢰할 수 없으므로 세션을 끊는다.
			LOGE("session %u recv stream is unusable (%s). dropping the session",
				clientSession->GetSessionID(),
				readResult == PacketReadResult::Invalid ? "invalid packet size" : "packet pool exhausted");
			releaseSessionAfterHandling = true;
			break;
		}

		if (!IsValidPacketId(packetId))
		{
			// 외부 입력으로 트리거 가능한 검증 실패다.
			// 스트림이 어긋났거나 악의적인 입력이므로 디스패치하지 않고 세션을 끊는다.
			// (이전에는 로그 없이 진행해서 잘못된 패킷ID 가 그대로 디스패치됐다)
			MEMORY_POOL::ReleasePacket(*GetPacketMemoryPool(), *GetGeneralMemoryPool(), packetDataByMemoryPool);
			LOGE("session %u received an invalid packet id %u (size %u). dropping the session",
				clientSession->GetSessionID(), packetId, packetSize);
			releaseSessionAfterHandling = true;
			break;
		}

		if (IsSystemPacketId(packetId))
		{
			const SystemPacketResult systemResult = HandleSystemPacket(clientSession, packetId, packetDataByMemoryPool, packetSize);

			MEMORY_POOL::ReleasePacket(*GetPacketMemoryPool(), *GetGeneralMemoryPool(), packetDataByMemoryPool);

			if (systemResult == SystemPacketResult::Rejected)
			{
				// 거절 응답을 이미 보냈다. 오류가 아니므로 ERROR 로 올리지 않고,
				// 세션은 정리한다. 이게 없으면 거절된 접속이 하트비트
				// 타임아웃까지 슬롯을 붙든다.
				LOGI("session %u rejected by the engine (PacketID : %u), releasing it",
					clientSession->GetSessionID(), packetId);
				releaseSessionAfterHandling = true;
				break;
			}

			if (systemResult != SystemPacketResult::Ok)
			{
				LOGE("session %u engine packet handling failed (PacketID : %u)", clientSession->GetSessionID(), packetId);
				releaseSessionAfterHandling = true;
				break;
			}

			continue;
		}

		if (!clientSession->IsEstablished())
		{
			MEMORY_POOL::ReleasePacket(*GetPacketMemoryPool(), *GetGeneralMemoryPool(), packetDataByMemoryPool);
			LOGW("session %u service packet received before session established (PacketID : %u)", clientSession->GetSessionID(), packetId);
			releaseSessionAfterHandling = true;
			break;
		}

		// 패킷 완성! 실제 처리 호출
		OnReceive(clientSession, packetId, packetDataByMemoryPool, packetSize);
	}

	// 처리 도중 반납이 예약됐을 수 있다. 시스템 패킷 응답을 보내려다
	// WSASend 가 실패하면 HandleSocketError -> NotifyDisconnect 로 이어져
	// 이 핸들러가 도는 동안 예약이 선다. 그 상태에서 다음 수신을 걸면
	// 곧 취소될 I/O 를 하나 더 만들 뿐이다.
	//
	// 예약이 이미 서 있으면 아래에서 반납을 다시 요청하지 않는다.
	// 요청해 봐야 poolState 가 이미 RELEASING 이라 "release skipped" 만 남는다.
	if (!releaseSessionAfterHandling && !clientSession->IsReleasePending())
	{
		// 다시 다음 수신 요청
		// 실패하면 이 세션은 pending recv 가 없는 상태로 남아
		// 하트비트 타임아웃까지 슬롯만 차지하는 좀비가 된다.
		// 그러므로 즉시 반납한다. (이전에는 로그만 남기고 방치했다)
		if (!clientSession->PostReceive())
		{
			LOGE("session %u failed to post the next recv, releasing the session instead of leaving it idle",
				clientSession->GetSessionID());
			releaseSessionAfterHandling = true;
		}
	}

	// 반납은 먼저 요청하고, 카운트는 마지막에 내려놓는다.
	//
	// 순서가 반대였다. 카운트를 먼저 내리면 그 자리에서 반납이 마무리되고
	// (지연 반납은 기다리지 않는다) 세션이 프리 리스트로 돌아간다. 그 뒤의
	// ReleaseClientSession 은 이미 남의 것이 된 세션을 반납하려 든다.
	//
	// 요청을 먼저 하면 우리가 카운트를 들고 있는 동안 예약 상태로 머물고,
	// 바로 아래 DecrementIO 가 0 으로 내리는 순간 마무리된다.
	if (releaseSessionAfterHandling)
	{
		m_sessionManager->ReleaseClientSession(clientSession);
	}

	// 이 함수의 마지막 줄이어야 한다. 이후로 clientSession 을 만지면 안 된다.
	clientSession->DecrementIO();
}

void IOCPServer::HandleRecvCancelled(OverlappedEx* overlappedEx, ClientSession* session)
{
	if (!session)
		return;

	if (!overlappedEx)
		return;

	// 세션 아이디를 먼저 읽어 둔다. DecrementIO 가 마지막 카운트를 내리면
	// 예약된 반납이 그 자리에서 끝나고, 세션은 다른 접속에 재배포될 수 있다.
	// 그 뒤에 GetSessionID() 를 부르면 남의 아이디를 찍는다.
	const uint32_t sessionId = session->GetSessionID();

	session->DecrementIO();

	LOGI("session %u recv cancelled", sessionId);
}

void IOCPServer::HandleSend(OverlappedEx* overlappedEx, ClientSession* session, DWORD bytesTransferred)
{
	if (!session)
		return;

	if (!overlappedEx)
		return;

	// 큐에는 ClientSession 만 들어가므로 RTTI 조회가 필요하지 않다.
	ClientSession* clientSession = static_cast<ClientSession*>(session);

	if (clientSession->GetSessionRole() != SESSION_ROLE::SERVER)
	{
		ENGINE_VIOLATION("session %u send completion arrived but the role is %d, not SERVER",
			clientSession->GetSessionID(), static_cast<int>(clientSession->GetSessionRole()));

		// 카운트를 반드시 내려놓아야 한다. 그러지 않으면 이 세션은
		// 영구히 취소 대기에서 풀리지 않는다.
		clientSession->DecrementIO();
		return;
	}

	// 선언만 되어 있고 엔진이 부르지 않던 훅이다. OnReceive 와 대칭이 맞아야
	// 서비스가 송신 완료를 관측할 수 있다.
	OnSend(session, bytesTransferred);

	// OnSendCompleted 가 다음 패킷의 WSASend 까지 발행하므로,
	// 우리 몫의 카운트를 먼저 내려놓으면 그 사이 카운트가 0 이 되어
	// 다른 스레드의 취소 대기가 통과할 수 있다. 처리 후에 내린다.
	clientSession->OnSendCompleted(bytesTransferred);

	clientSession->DecrementIO();
}

void IOCPServer::HandleSendCancelled(OverlappedEx* overlappedEx, ClientSession* session)
{
	if (!session)
		return;

	if (!overlappedEx)
		return;

	// 아이디를 먼저 읽는 이유는 HandleRecvCancelled 와 같다.
	const uint32_t sessionId = session->GetSessionID();

	session->DecrementIO();

	LOGI("session %u send cancelled", sessionId);
}

void IOCPServer::HandleSessionDisconnected(OverlappedEx* overlappedEx, ClientSession* session, DWORD bytesTransferred)
{
	// 세션의 정상 종료 시퀀스

	ENGINE_CHECK_RETVOID(session != nullptr && overlappedEx != nullptr,
		"disconnect handler called with session %p overlapped %p", session, overlappedEx);

	if (overlappedEx->operation == IO_OPERATION::RECV)
	{
		// 여기서 OnClientDisconnect 를 부르지 않는다.
		//
		// 예전에는 이 자리가 유일한 종료 통지였다. 그래서 정상 종료(FIN)로
		// 끝난 세션만 통지를 받았고, RST / 하트비트 타임아웃 / 파싱 실패 /
		// 접속 시퀀스 실패 / 서버 종료로 끝난 세션은 아무 말 없이 사라졌다.
		// 이제 반납이 실제로 일어나는 곳(ClientSessionPool)에서 한 번 부른다.

		// 반납을 먼저 요청하고 카운트는 마지막에 내린다.
		//
		// 예전에는 반대였다. ReleaseClientSession 이 카운트 0 을 기다렸기
		// 때문에, 우리 몫을 먼저 내려놓지 않으면 자기를 기다리는 꼴이
		// 됐다. 이제 기다리지 않으므로 그 제약이 사라졌고, 오히려 먼저
		// 내리면 반납이 그 자리에서 끝나 아래 호출이 남의 세션을 건드린다.
		m_sessionManager->ReleaseClientSession(session);

		// RECV I/O 가 걸려 있던 세션이므로 그 몫을 여기서 내린다.
		// 이 분기의 마지막 줄이어야 한다.
		session->DecrementIO();
	}
	else
	{
		// 0바이트 완료는 RECV 에서만 발생한다.
		ENGINE_VIOLATION("session %u reported a zero byte completion on io %d, which should only happen for RECV",
			session->GetSessionID(), static_cast<int>(overlappedEx->operation));

		session->DecrementIO();
	}
}

bool IOCPServer::CreateListenSocket(const char* ipAddress, uint16_t port)
{
	m_serverSocket = ::WSASocket(AF_INET, SOCK_STREAM, IPPROTO_TCP, NULL, 0, WSA_FLAG_OVERLAPPED);

	if (m_serverSocket == INVALID_SOCKET)
	{
		//Log::log(LogLevel::LOG_ERROR, "[%s] WSASocket Listen socket failed");
		return false;
	}


	//Log::log(LogLevel::LOG_INFO, "[%s] WSASocket Listen socket success");

	// Listen 소켓도 IOCP에 연결
	if (CreateIoCompletionPort((HANDLE)m_serverSocket, GetIOCPHandle(), (ULONG_PTR)m_serverSocket, 0) == NULL)
	{
		//Log::log(LogLevel::LOG_ERROR, "[%s] CreateIoCompletionPort failed for listen socket - ErroCode : %d", WSAGetLastError());

		return false;
	}

	//Log::log(LogLevel::LOG_INFO, "[%s] CreateIoCompletionPort success for listen socket");

	return true;
}

void IOCPServer::DestroyListenSocket()
{
	IOCPCore::CloseSocketHandle(m_serverSocket);

	m_serverSocket = INVALID_SOCKET;
}

bool IOCPServer::BindServerSocket(SOCKET serverSocket, const char* ipAddress, uint16_t port)
{
	SOCKADDR_IN serverAddr = {};
	serverAddr.sin_family = AF_INET;
	serverAddr.sin_port = ::htons(port);

	if (ipAddress == nullptr || ipAddress[0] == '\0')
	{
		serverAddr.sin_addr.s_addr = ::htonl(INADDR_ANY);
	}
	else
	{
		const int nResult = ::inet_pton(AF_INET, ipAddress, &serverAddr.sin_addr);
		if (nResult != 1)
		{
			LOGE("invalid bind address : %s", ipAddress);
			return false;
		}
	}

	if (::bind(serverSocket, (SOCKADDR*)&serverAddr, sizeof(serverAddr)) == SOCKET_ERROR)
	{
		//Log::log(LogLevel::LOG_ERROR, "[%s] bind failed - ErroCode : %d", WSAGetLastError());

		return false;
	}

	//Log::log(LogLevel::LOG_INFO, "[%s] bind success");

	return true;
}

bool IOCPServer::ListenServerSocket(SOCKET serverSocket)
{
	if (::listen(serverSocket, SOMAXCONN) == SOCKET_ERROR)
	{
		//Log::log(LogLevel::LOG_ERROR, "[%s] listen failed - ErroCode : %d", WSAGetLastError());

		return false;
	}

	//Log::log(LogLevel::LOG_INFO, "[%s] listen success");

	return true;
}

bool IOCPServer::InitializeGUIDAcceptEx(SOCKET serverSocket)
{
	// AcceptEX 를 사용하기 위해 함수 포인터를 얻어오는 함수
	// WSAIoctl 를 사용하여 AcceptEx 함수 포인터를 얻어올 수 있다.

	DWORD bytes = 0;

	// AcceptEX 함수 포인터 얻기
	GUID GuidAcceptEx = WSAID_ACCEPTEX;
	if (::WSAIoctl(serverSocket,
		SIO_GET_EXTENSION_FUNCTION_POINTER,
		&GuidAcceptEx,
		sizeof(GuidAcceptEx),
		&m_acceptEx,
		sizeof(m_acceptEx),
		&bytes,
		nullptr,
		nullptr) == SOCKET_ERROR)
	{
		//Log::log(LogLevel::LOG_ERROR, "[%s] WSAIoctl Get AcceptEX Pointer Failed - ErroCode : %d", WSAGetLastError());

		return false;
	}

	// GetAcceptExSockaddrs 함수 포인터 얻기
	GUID GuidGetAddrs = WSAID_GETACCEPTEXSOCKADDRS;
	if (::WSAIoctl(serverSocket, SIO_GET_EXTENSION_FUNCTION_POINTER,
		&GuidGetAddrs, sizeof(GuidGetAddrs),
		&m_getAcceptExSockAddrs, sizeof(m_getAcceptExSockAddrs),
		&bytes, nullptr, nullptr) == SOCKET_ERROR)
	{
		//Log::log(LogLevel::LOG_ERROR, "[%s] WSAIoctl Get GetAcceptExSockaddrs Pointer Failed - ErroCode : %d", WSAGetLastError());

		return false;
	}


	return true;
}

void IOCPServer::FinalizeGUIDAcceptEx()
{
	m_acceptEx = nullptr;
	m_getAcceptExSockAddrs = nullptr;
}

bool IOCPServer::CancelAllAcceptIO()
{
	if (m_serverSocket == INVALID_SOCKET)
	{
		return true;
	}

	// lpOverlapped 가 null 이면
	// 모든 IO 작업을 취소시킨다.
	if (!::CancelIoEx(reinterpret_cast<HANDLE>(m_serverSocket), nullptr))
	{
		DWORD error = ::GetLastError();

		if (error == ERROR_NOT_FOUND)
		{
			// 미완료 I/O 가 없어서 취소할 것이 없는 경우
			// 아무것도 하지 않고 빠져나가면 된다.
			// 정상
			return true;
		}
		else if (error == ERROR_INVALID_HANDLE)
		{
			// 이미 닫혔거나 유효하지 않은 핸들인 경우
			// 무시 가능
			return true;
		}
		else if (error == ERROR_OPERATION_ABORTED)
		{
			// I/O 가 이미 완료 직전에 있어서
			// 취소되지 못한 경우 (드물다)
			return false;
		}
		else
		{
			return false;
		}
	}

	return true;
}

bool IOCPServer::PrepareAccept()
{
	// Listner Session Post Accept 걸어 주기
	const uint32_t maxAcceptSessionCount = m_sessionManager->GetAcceptSessionCount();

	for (uint32_t i = 0; i < maxAcceptSessionCount; i++)
	{
		AcceptSession* session = m_sessionManager->GetAcceptSession(i);

		if (!session)
		{
			LOGE("could not get the accept session from the pool");
			ENGINE_BREAK_IF_DEBUGGER();
			return false;
		}

		if (session->GetSessionRole() != SESSION_ROLE::ACCEPT)
		{
			LOGE("the session is not an accept session");

			ENGINE_BREAK_IF_DEBUGGER();
			return false;
		}

		if (session->GetAcceptSessionState() != AcceptSessionState::ACCEPT_READY)
		{
			LOGE("the accept session is not in ACCEPT_READY state");
			ENGINE_BREAK_IF_DEBUGGER();
			return false;
		}

		// 기동 시점이라 경쟁자는 없지만, 슬롯 소유권을 얻고 거는 규칙은
		// 발행부 전체에 예외 없이 적용한다.
		if (!session->TryAcquireSlot())
		{
			ENGINE_VIOLATION("accept session %u is already owned before the first post", i);
			return false;
		}

		if (!PostAccept(session))
		{
			session->ReleaseSlot();
			LOGE("PostAccept failed for the accept session");
			ENGINE_BREAK_IF_DEBUGGER();
			return false;
		}
	}

	return true;
}

bool IOCPServer::PrepareAccept(uint32_t sessionId)
{
	AcceptSession* session = m_sessionManager->GetAcceptSession(sessionId);

	if (!session)
	{
		LOGE("could not get the accept session from the pool");

		ENGINE_BREAK_IF_DEBUGGER();
		return false;
	}

	if (session->GetSessionRole() != SESSION_ROLE::ACCEPT)
	{
		LOGE("the session is not an accept session");

		ENGINE_BREAK_IF_DEBUGGER();
		return false;
	}

	if (session->GetAcceptSessionState() != AcceptSessionState::ACCEPT_READY)
	{
		LOGE("the accept session is not in ACCEPT_READY state");
		ENGINE_BREAK_IF_DEBUGGER();
		return false;
	}

	if (!session->TryAcquireSlot())
	{
		// 다른 쪽이 이미 이 슬롯을 쓰고 있다.
		LOGW("accept session %u is already owned, skipping the post", sessionId);
		return false;
	}

	if (!PostAccept(session))
	{
		session->ReleaseSlot();
		LOGE("PostAccept failed for the accept session");

		return false;
	}

	return true;
}

bool IOCPServer::PostAccept(AcceptSession* acceptSession)
{
	if (!acceptSession)
	{
		LOGE("PostAccept was given no accept session");

		return false;
	}

	LOGT("accept session %u posting AcceptEx", acceptSession->GetSessionID());

	if (::InterlockedCompareExchange(&m_serverShutdownRequested, 0, 0) == TRUE)
	{
		// 이미 서버가 Shutdown 모드면 이 Accept 결과는 버린다.

		IOCPCore::CloseSocketHandle(acceptSession->DetachSocket());

		if (!acceptSession->OnDisconnect())
		{
			ENGINE_VIOLATION("accept session %u OnDisconnect reported failure", acceptSession->GetSessionID());
		}

		acceptSession->ResetSession();

		return false;
	}

	// 예전에는 여기서 3번까지 즉시 재시도하며 자원 부족일 때 ::Sleep(10) 을
	// 했다. 이 함수는 IOCP 완료 핸들러 안에서 불리므로 그 대기가 워커
	// 스레드를 통째로 막는다. 자원 부족은 보통 여러 슬롯에 동시에 오기
	// 때문에 워커 여러 개가 같이 잠들 수 있다.
	//
	// 게다가 즉시 재시도는 의미가 없다. WSAENOBUFS 가 10ms 안에 풀릴
	// 이유가 없기 때문이다. 그래서 한 번만 시도하고 실패하면 슬롯을 비운
	// 채로 물러난다. 비워진 슬롯은 주기 점검(RefillAcceptSlots)이 채운다.
	{
		// 클라이언트와 연결될 소켓 생성
		SOCKET clientSocket = ::WSASocket(AF_INET, SOCK_STREAM, 0, NULL, 0, WSA_FLAG_OVERLAPPED);
		if (clientSocket == INVALID_SOCKET)
		{
			LOGE("accept session %u WSASocket failed (error %d)", acceptSession->GetSessionID(), ::WSAGetLastError());

			return false;
		}

		LOGT("accept session %u created socket %d", acceptSession->GetSessionID(), (int)clientSocket);

		// 세션에 클라이언트 소켓을 등록
		acceptSession->SetClientSocket(clientSocket);

		// IO 수량 증가(AcceptEx 에 대한 IO)
		acceptSession->IncrementIO();

		// Overlapped 구조체 초기화
		OverlappedEx& overlappedEx = acceptSession->GetAcceptOverlapped();  // session이 미리 생성한 OverlappedEx 포인터 반환

		// I/O 를 거는 쪽이 자기 OVERLAPPED 를 직접 준비한다. 이전 완료가
		// 남긴 Internal / hEvent 를 그대로 재사용하지 않도록 지운다.
		// 예전에는 호출부가 ResetSession 으로 이 일을 대신했는데, 그건
		// 정리 경로용 함수라 정상 수락마다 "IO 가 남아 있다" 는 오류 로그를
		// 찍었다. 그 시점 카운트 1 은 방금 완료된 AcceptEx 의 것이라 정상이다.
		//
		// AcceptEx 는 주소 버퍼를 따로 받으므로 wsaBuffer 는 비워 둔다.
		overlappedEx.ResetForNextIO(acceptSession->GetSessionID());

		acceptSession->SetAcceptSessionState(AcceptSessionState::ACCEPT_WAIT);

		// AcceptEx 버퍼: 주소 정보를 담기 위해 충분한 크기 필요
		DWORD bytesReceived = 0;

		BOOL bResult = m_acceptEx(
			m_serverSocket,
			clientSocket,
			acceptSession->GetAcceptBuffer(),       // 2 * (addr + padding)
			0,                            // 0 으로 설정하면 데이터가 도착하기 전까지 대기하지 않고 연결만 완료시킨다.
			sizeof(SOCKADDR_IN) + 16,
			sizeof(SOCKADDR_IN) + 16,
			&bytesReceived,
			(LPOVERLAPPED)&overlappedEx         // OVERLAPPED 구조체
		);

		if (bResult == FALSE)
		{
			const int nError = ::WSAGetLastError();
			if (nError != ERROR_IO_PENDING)
			{
				// AcceptEx 실패 했으므로 IO 수량 감소
				acceptSession->DecrementIO();
				acceptSession->SetAcceptSessionState(AcceptSessionState::ACCEPT_ABORTED);

				IOCPCore::CloseSocketHandle(acceptSession->DetachSocket());
				acceptSession->ResetSession();

				if (nError == WSAECONNRESET)
				{
					LOGW("accept session %u : the peer disconnected during AcceptEx", acceptSession->GetSessionID());
				}
				else if (nError == WSAENOBUFS || nError == WSAEMFILE)
				{
					// 워커를 재우지 않는다. 이 슬롯은 비운 채로 두고
					// 주기 점검이 다시 채우게 맡긴다.
					LOGE("accept session %u AcceptEx hit a system resource shortage (error %d). the slot is left empty for the periodic refill",
						acceptSession->GetSessionID(), nError);
				}
				else if (nError == ERROR_OPERATION_ABORTED)
				{
					LOGW("accept session %u AcceptEx aborted", acceptSession->GetSessionID());
				}
				else
				{
					LOGE("accept session %u AcceptEx failed (error %d)", acceptSession->GetSessionID(), nError);
				}

				return false;
			}
		}
	}

	// 여기까지 왔으면 AcceptEx 가 걸렸다 (ERROR_IO_PENDING 또는 즉시 성공).
	::InterlockedIncrement(&m_postedAcceptCount);

	return true;
}

void IOCPServer::RefillAcceptSlots()
{
	if (!m_sessionManager)
		return;

	if (::InterlockedCompareExchange(&m_serverShutdownRequested, 0, 0) == TRUE)
		return;

	const uint32_t slotCount = m_sessionManager->GetAcceptSessionCount();
	uint32_t refilled = 0;

	for (uint32_t i = 0; i < slotCount; ++i)
	{
		AcceptSession* acceptSession = m_sessionManager->GetAcceptSession(i);
		if (!acceptSession)
			continue;

		// 예전에는 GetAcceptSessionState() == ACCEPT_READY 로 판단했다.
		// 그 상태는 원자적이지 않고, 워커가 ResetSession 과 PostAccept 사이에
		// 있을 때도 ACCEPT_READY 로 보인다. 그래서 둘이 함께 걸었다.
		//
		// 이제 근거는 이 CAS 하나다. 지면 다른 쪽이 그 슬롯을 쓰고 있다는
		// 뜻이므로 조용히 넘어간다 - 오류가 아니라 정상이다.
		if (!acceptSession->TryAcquireSlot())
			continue;

		if (!PostAccept(acceptSession))
		{
			// 아직 자원이 부족하다. 슬롯을 놓고 다음 주기에 다시 시도한다.
			acceptSession->ReleaseSlot();
			break;
		}

		++refilled;
	}

	if (refilled > 0)
	{
		LOGI("refilled %u accept slot(s), now %u of %u posted",
			refilled, GetPostedAcceptCount(), GetDesiredAcceptCount());

		// 다시 채워졌으므로 다음 고갈 때 또 울릴 수 있도록 래치를 푼다.
		::InterlockedExchange(&m_acceptStarvationReported, FALSE);
	}
}

void IOCPServer::ReportAcceptStarvationIfNeeded()
{
	if (::InterlockedCompareExchange(&m_serverShutdownRequested, 0, 0) == TRUE)
		return;

	if (::InterlockedCompareExchange(&m_postedAcceptCount, 0, 0) != 0)
		return;

	// 같은 고갈에 대해 한 번만 울린다. RefillAcceptSlots 가 성공하면 풀린다.
	if (::InterlockedCompareExchange(&m_acceptStarvationReported, TRUE, FALSE) != FALSE)
		return;

	ENGINE_VIOLATION("no AcceptEx is posted. the server is running but will not take any new connection until a slot is refilled");
}

uint32_t IOCPServer::GetPostedAcceptCount() const
{
	const LONG count = ::InterlockedCompareExchange(
		const_cast<volatile LONG*>(&m_postedAcceptCount), 0, 0);

	return count < 0 ? 0 : static_cast<uint32_t>(count);
}

uint32_t IOCPServer::GetDesiredAcceptCount() const
{
	return m_desiredAcceptCount;
}

uint32_t IOCPServer::GetOutstandingIOCount() const
{
	if (!m_sessionManager)
		return 0;

	return m_sessionManager->GetOutstandingIOCount();
}

void IOCPServer::OnSessionDisconnectNotify(void* context, ClientSession* session)
{
	static_cast<IOCPServer*>(context)->OnClientDisconnect(session);
}

uint32_t IOCPServer::GetInUseSessionCount() const
{
	if (!m_sessionManager)
		return 0;

	return m_sessionManager->GetClientSessionInUseCount();
}

ReadySessionQueue* IOCPServer::GetReadySessionQueue() const
{
	return m_readySessionQueue;
}

PacketHandlerTable* IOCPServer::GetPacketHandlerTable() const
{
	return m_packetHandlerTable;
}

EngineMemoryPool* IOCPServer::GetJobMemoryPool() const
{
	return m_jobMemoryPool;
}

EngineMemoryPool* IOCPServer::GetPacketMemoryPool() const
{
	return m_packetMemoryPool;
}

EngineMemoryPool* IOCPServer::GetGeneralMemoryPool() const
{
	return m_generalMemoryPool;
}

const HandlerContext& IOCPServer::GetHandlerContext() const
{
	return m_handlerContext;
}

bool IOCPServer::SubmitPacketJob(ISession* session, uint16_t packetId, const char* packetData, uint32_t packetSize)
{
	// recv 완료 키에 등록되는 것은 ClientSession 뿐이므로 이 경로의 세션은
	// 항상 ClientSession 이다.
	ClientSession* clientSession = static_cast<ClientSession*>(session);

	// 어떤 이유로 거부하든 패킷은 여기서 회수한다. 아래 모든 실패 경로가
	// 이 람다를 지난다 — 하나라도 빠지면 그게 곧 누수다.
	auto dropPacket = [this, packetData]()
		{
			if (packetData && m_packetMemoryPool && m_generalMemoryPool)
			{
				MEMORY_POOL::ReleasePacket(*m_packetMemoryPool, *m_generalMemoryPool, packetData);
			}
		};

	if (!clientSession || !packetData)
	{
		ENGINE_VIOLATION("SubmitPacketJob called with session %p packet %p",
			static_cast<const void*>(session), static_cast<const void*>(packetData));
		dropPacket();
		return false;
	}

	if (!m_packetHandlerTable || !m_jobMemoryPool || !m_readySessionQueue)
	{
		ENGINE_VIOLATION("session %u cannot submit a job : the server is not fully initialized",
			clientSession->GetSessionID());
		dropPacket();
		return false;
	}

	PacketHandlerFunc handler = m_packetHandlerTable->GetHandler(packetId);
	if (!handler)
	{
		LOGW("session %u received packet id %u with no registered handler, dropping it",
			clientSession->GetSessionID(), packetId);
		dropPacket();
		return false;
	}

	Job* job = MEMORY_POOL::CreateJob(*m_jobMemoryPool);
	if (!job)
	{
		LOGE("session %u could not allocate a job, dropping packet id %u",
			clientSession->GetSessionID(), packetId);
		dropPacket();
		return false;
	}

	job->SetPacketJob(JobType::PACKET, handler, session, packetId, packetData, packetSize, m_handlerContext);

	// wasEmpty 는 받지만 스케줄 판단에 쓰지 않는다. 이유는 아래에.
	bool wasEmpty = false;
	if (!clientSession->GetJobQueue().EnqueueJob(job, wasEmpty))
	{
		LOGE("session %u failed to enqueue a job for packet id %u",
			clientSession->GetSessionID(), packetId);

		dropPacket();
		MEMORY_POOL::ReleaseJob(*m_jobMemoryPool, job);
		return false;
	}

	// 여기부터 packetData 와 job 의 소유권은 큐에 있다.

	// 스케줄 조건에서 wasEmpty 를 뺐다.
	//
	// 예전 규약은 "큐가 비어 있었을 때만 스케줄한다" 였다. 대개는 맞지만
	// 복구 불가 상태를 하나 만든다 — ReadySessionQueue::Push 가 실패하면
	// 큐에는 Job 이 남고 스케줄은 되지 않은 상태가 된다. 그 뒤에 오는
	// 패킷은 wasEmpty == false 를 보고 스케줄을 건너뛰므로, 그 세션은
	// 영구히 스케줄되지 않는다.
	//
	// 중복 Push 를 실제로 막는 것은 아래 CAS 다. wasEmpty 는 그 CAS 를
	// 아끼는 최적화였을 뿐이고, 그 최적화가 위 상태를 복구 불가로 만든다.
	// CAS 만 남기면 패킷마다 다시 시도하므로 일시적 Push 실패가 스스로
	// 복구된다. 값은 원자 연산 하나(패킷당 수십 cycle)이고, 실측된 잡당
	// 비용(약 32us)에 비하면 무시할 수준이다.
	if (!clientSession->IsProcessingReady())
	{
		// 이미 스케줄되어 있거나 워커가 처리 중이다. 그 워커가 드레인을
		// 끝낸 뒤 큐가 비어 있지 않으면 스스로 다시 올린다.
		return true;
	}

	if (!m_readySessionQueue->Push(clientSession))
	{
		// 반드시 되돌린다. 그러지 않으면 이 세션은 처리 중으로 표시된 채
		// 아무도 처리하지 않는 상태가 된다.
		clientSession->UpdateProcessingFlag(0);

		LOGE("session %u could not be scheduled : the ready queue is full. the job stays queued",
			clientSession->GetSessionID());
	}

	return true;
}

void* IOCPServer::GetServiceContext()
{
	return this;
}

bool IOCPServer::SendSystemAuthResponse(ClientSession* session, SYSTEM_AUTH_RESULT authResult)
{
	if (!session || !session->IsTransportConnected())
	{
		return false;
	}

	void* memory = MEMORY_POOL::CreatePacket(*GetPacketMemoryPool(), sizeof(SC_SYSTEM_AUTH_RESPONSE_PACKET));
	if (!memory)
	{
		return false;
	}

	SC_SYSTEM_AUTH_RESPONSE_PACKET* response = reinterpret_cast<SC_SYSTEM_AUTH_RESPONSE_PACKET*>(memory);
	*response = SC_SYSTEM_AUTH_RESPONSE_PACKET();
	response->authResult = static_cast<uint16_t>(authResult);

	void* packetData = response;
	if (!session->EnqueueSendPacket(&packetData, sizeof(SC_SYSTEM_AUTH_RESPONSE_PACKET)))
	{
		MEMORY_POOL::ReleasePacket(*GetPacketMemoryPool(), *GetGeneralMemoryPool(), response);
		return false;
	}

	return true;
}

IOCPServer::SystemPacketResult IOCPServer::HandleSystemPacket(ClientSession* session, uint16_t packetId, const char* packetData, uint32_t packetSize)
{
	if (!session || !packetData)
	{
		return SystemPacketResult::Failed;
	}

	switch (static_cast<PACKET_ID>(packetId))
	{
	case PACKET_ID::CS_SYSTEM_AUTH_REQUEST:
	{
		if (packetSize != sizeof(CS_SYSTEM_AUTH_REQUEST_PACKET))
		{
			return SystemPacketResult::Failed;
		}

		const CS_SYSTEM_AUTH_REQUEST_PACKET* request = reinterpret_cast<const CS_SYSTEM_AUTH_REQUEST_PACKET*>(packetData);
		if (session->GetServerSessionState() != ServerSessionState::CONNECTED &&
			session->GetServerSessionState() != ServerSessionState::AUTH_PENDING)
		{
			SendSystemAuthResponse(session, SYSTEM_AUTH_RESULT::INVALID_STATE);
			return SystemPacketResult::Rejected;
		}

		if (request->protocolVersion != IOCP_ENGINE_PROTOCOL_VERSION)
		{
			SendSystemAuthResponse(session, SYSTEM_AUTH_RESULT::PROTOCOL_MISMATCH);
			return SystemPacketResult::Rejected;
		}

		// 서비스가 붙잡아 둘 상한을 넘었는지 여기서 본다.
		//
		// accept 단계가 아니라 인증 단계에서 거절하는 이유는, 그 시점에는
		// 소켓이 아직 세션에 붙지 않아 응답을 보낼 수단이 없기 때문이다.
		// 여기까지 오면 정상 세션이므로 기존 인증 응답 경로를 그대로 쓴다.
		if (m_connectionPolicy.serviceCapacity > 0)
		{
			const uint32_t inUse = m_sessionManager->GetClientSessionInUseCount();

			if (inUse > m_connectionPolicy.serviceCapacity)
			{
				LOGW("session %u rejected : service capacity reached (%u in use, limit %u)",
					session->GetSessionID(), inUse, m_connectionPolicy.serviceCapacity);

				SendSystemAuthResponse(session, SYSTEM_AUTH_RESULT::SERVER_FULL);

				return SystemPacketResult::Rejected;
			}
		}

		session->SetServerSessionState(ServerSessionState::AUTH_PENDING);

		if (!SendSystemAuthResponse(session, SYSTEM_AUTH_RESULT::SUCCESS))
		{
			return SystemPacketResult::Failed;
		}

		session->SetServerSessionState(ServerSessionState::ESTABLISHED);
		LOGI("session %u session established", session->GetSessionID());
		return SystemPacketResult::Ok;
	}

	case PACKET_ID::CS_SYSTEM_HEARTBEAT_RESPONSE:
	{
		if (packetSize != sizeof(CS_SYSTEM_HEARTBEAT_RESPONSE_PACKET))
		{
			return SystemPacketResult::Failed;
		}

		if (!session->IsEstablished())
		{
			return SystemPacketResult::Failed;
		}

		session->UpdateLastHeartbeatTick();
		return SystemPacketResult::Ok;
	}

	default:
		LOGW("session %u unhandled system packet id: %u", session->GetSessionID(), packetId);
		return SystemPacketResult::Failed;
	}
}
