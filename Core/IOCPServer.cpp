#include "IOCPServer.h"
#include "HeartbeatThread.h"

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

#include "../Memory/SlabMemoryPool.h"
#include "../Memory/SlabMemoryPoolHelper.h"

#include "../Network/SocketOption.h"

#include "../Buffer/RecvPacketBuffer.h"
#include "../Buffer/HybridSendPacketPool.h"
#include "../Buffer/PreDefine.h"
#include "../Protocol/PacketID.h"

#include "../Session/ISession.h"
#include "../Session/SessionManager.h"
#include "../Session/ClientSession.h"
#include "../Session/AcceptSession.h"

#include "../../CoreLib/Utils/Logger.h"

#pragma comment(lib, "ws2_32.lib") // for WinSock2
#pragma comment(lib, "mswsock.lib") // for AcceptEX / ConnectEx

using namespace CoreLibrary::Utils;

IOCPServer::IOCPServer()
{
}

IOCPServer::~IOCPServer()
{
	StopServer();
}

bool IOCPServer::StartServer(const char* ipAddress, const uint16_t port, const uint32_t maxConnectionCount)
{
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
		{MEMORY_SIZE_1K, 1024},
		{MEMORY_SIZE_2K, 1024},
		{MEMORY_SIZE_4K, 1024},
		{MEMORY_SIZE_8K, 512},
		{MEMORY_SIZE_16K, 512},
		{MEMORY_SIZE_32K, 512},
	};

	m_packetMemoryPool = new SlabMemoryPool;
	if (!m_packetMemoryPool)
		return false;

	m_packetMemoryPool->Initialize(configsPacket, _countof(configsPacket));

	SlabMemoryPool::SlabConfig configsImageBuffer[] = {
		{MEMORY_SIZE_1MB, 1},
		//{MEMORY_SIZE_4MB, 1},
		//{MEMORY_SIZE_8MB, 1}
	};

	m_generalMemoryPool = new SlabMemoryPool;
	if (!m_generalMemoryPool)
		return false;

	m_generalMemoryPool->Initialize(configsImageBuffer, _countof(configsImageBuffer));

	m_hybridSendPacketPool = new HybridSendPacketPool();
	if (!m_hybridSendPacketPool)
		return false;
	if (!m_hybridSendPacketPool->Initialize(HYBRID_SEND_PACKET_POOL_SIZE))
		return false;

	m_readySessionQueue = new ReadySessionQueue;
	if (!m_readySessionQueue)
		return false;

	const uint32_t sessionQueueBufferCount = maxConnectionCount/* * 2*/;
	m_readySessionQueue->Initialize(sessionQueueBufferCount);

	m_handlerContext.jobMemoryPool = GetJobMemoryPool();
	m_handlerContext.packetMemoryPool = GetPacketMemoryPool();
	m_handlerContext.generalMemoryPool = GetGeneralMemoryPool();

	m_packetHandlerTable = new PacketHandlerTable(m_handlerContext);
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
	m_readySessionScheduler->Initialize(sessionProcessorThreadCount, m_readySessionQueue, m_jobMemoryPool, m_packetMemoryPool, m_generalMemoryPool);

	m_sessionManager = new SessionManager;
	if (!m_sessionManager)
		return false;

	//::GetMaximumProcessorCount(ALL_PROCESSOR_GROUPS) / 2;
	if (!m_sessionManager->Initialize(1, maxConnectionCount, m_hybridSendPacketPool, m_jobMemoryPool, m_packetMemoryPool, m_generalMemoryPool, IOCPCore::CloseSocketHandle))
		return false;

	if (!PrepareAccept())
	{
		return false;
	}

	constexpr uint64_t HeartbeatCheckInterval_ms = 5'000;
	constexpr uint64_t HeartbeatTimeout_ms = 15'000;
	m_heartbeatThread = new HeartbeatThread(m_sessionManager, HeartbeatCheckInterval_ms, HeartbeatTimeout_ms);
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
	Logger::Log(LogLevel::LOG_INFO, "[%s] Server Shutdown!", __FUNCTION__);

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
		Logger::Log(LogLevel::LOG_INFO, "[%s] 모든 AcceptEx IO 취소 요청", __FUNCTION__);
		m_sessionManager->RequestAllAcceptIOCancel();

		Logger::Log(LogLevel::LOG_INFO, "[%s] 모든 AcceptEx IO 취소 처리 대기 중...", __FUNCTION__);
		m_sessionManager->WaitForAllAcceptIOCancelComplete(10'000);
	}

	if (m_sessionManager)
	{
		Logger::Log(LogLevel::LOG_INFO, "[%s] 모든 Recv/Send IO 취소 요청", __FUNCTION__);
		m_sessionManager->RequestAllRecvSendIOCancel();

		Logger::Log(LogLevel::LOG_INFO, "[%s] 모든 Recv/Send IO 취소 처리 대기 중...", __FUNCTION__);
		m_sessionManager->WaitForAllRecvSendIOCancelComplete(10'000);

		Logger::Log(LogLevel::LOG_INFO, "[%s] 모든 ClientSession socket Close", __FUNCTION__);
		m_sessionManager->DisconnectAllSessions();
	}

	// 서버 소켓의 모든 Accept I/O 를 취소시킨다.
	// AcceptEx 를 사용하므로 모든 세션이 이미 Accept GQCS 에 등록되어 있다.
	Logger::Log(LogLevel::LOG_INFO, "[%s] 서버 리슨 소켓 닫기", __FUNCTION__);
	if (!CancelAllAcceptIO())
	{
		__debugbreak();
	}

	//if (m_sessionManager)
	//{
	//	Logger::Log(LogLevel::LOG_INFO, "[%s] 모든 AcceptEx IO 취소 처리 대기 중...", __FUNCTION__);
	//	m_sessionManager->WaitForAllAcceptIOCancelComplete(10'000);
	//}


	// 서버 리슨 소켓을 닫아서 새 클라이언트 연결이 들어오는걸 먼저 막아야 한다.
	Logger::Log(LogLevel::LOG_INFO, "[%s] 서버 리슨 소켓을 닫는 중...", __FUNCTION__);
	DestroyListenSocket();

	FinalizeGUIDAcceptEx();

	// 연결 되어 있는 세션을 강제로 Disconnect 하고 클라이언트 소켓을 닫는다.
	Logger::Log(LogLevel::LOG_INFO, "[%s] 연결되어 있는 세션을 강제로 Disconnect 중...", __FUNCTION__);

	Logger::Log(LogLevel::LOG_INFO, "[%s] IOCP Core Shutdown 중...", __FUNCTION__);
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

	if (m_hybridSendPacketPool)
	{
		m_hybridSendPacketPool->Finalize();
		delete m_hybridSendPacketPool;
		m_hybridSendPacketPool = nullptr;
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
}

void IOCPServer::HandleCompletion(
	ULONG_PTR completionKey,
	LPOVERLAPPED overlapped,
	DWORD bytesTransferred,
	BOOL completionStatus)
{
	OverlappedEx* overlappedEx = reinterpret_cast<OverlappedEx*>(overlapped);

	if (overlappedEx == nullptr)
	{
		__debugbreak();
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
		ISession* session = reinterpret_cast<ISession*>(completionKey);
		if (session == nullptr)
		{
			__debugbreak();
			return;
		}

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
		__debugbreak();
		return;
	}
}

void IOCPServer::HandleSocketError(OverlappedEx* overlappedEx, ISession* session, int errorCode, IO_OPERATION ioOperation)
{
	// 공용으로 처리 되어야 하는 예외 처리

	if (ioOperation == IO_OPERATION::ACCEPT)
	{
		Logger::Log(LogLevel::LOG_INFO, "[%s][IO : %d] GQCS Failed Error SEND", __FUNCTION__, (int)ioOperation);

		if (errorCode == ERROR_OPERATION_ABORTED)
		{
			// CancelIoEx 로 인한 I/O 를 강제로 취소한 경우
			// 또는 서버 리슨 소켓을 닫은 경우
			// 이전에 걸어 두었던 Accept 에 대해 취소된 IO 작업 완료 알림 수신

			HandleAcceptIOCancelled(overlappedEx->sessionId);

			return;
		}

		__debugbreak();

		return;
	}
	else if (ioOperation == IO_OPERATION::RECV)
	{
		if (session == nullptr)
		{
			__debugbreak();
			return;
		}

		Logger::Log(LogLevel::LOG_INFO, "[%s][ClientSession : %d][IO : %d] GQCS Failed Error RECV", __FUNCTION__, session->GetSessionID(), (int)ioOperation);

		switch (errorCode)
		{
		case WSAECONNRESET:       // 연결이 비정상 종료됨 (상대방 강제 종료)
		case WSAECONNABORTED:     // 연결 중단됨
		case WSAENOTCONN:         // 연결이 이미 끊김
		case WSAESHUTDOWN:        // 소켓 송수신 불가
		case ERROR_NETNAME_DELETED: // 네트워크 이름 삭제됨 (연결 끊김)
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
		if (session == nullptr)
		{
			__debugbreak();
			return;
		}

		Logger::Log(LogLevel::LOG_INFO, "[%s][ClientSession : %d][IO : %d] GQCS Failed Error SEND", __FUNCTION__, session->GetSessionID(), (int)ioOperation);

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

void IOCPServer::HandleAccept(uint32_t sessionId, DWORD bytesTransferred)
{
	Logger::Log(LogLevel::LOG_INFO, "[%s][%d] HandleAccept", __FUNCTION__, sessionId);

	ISession* session = m_sessionManager->GetAcceptSession(sessionId);
	AcceptSession* acceptSession = dynamic_cast<AcceptSession*>(session);

	if (!acceptSession)
	{
		Logger::Log(LogLevel::LOG_ERROR, "[%s] AcceptEx 시작하려고 세션을 캐스팅 했는데 nullptr", __FUNCTION__);

		__debugbreak();
		return;
	}

	acceptSession->OnAccept();

	if (::InterlockedCompareExchange(&m_serverShutdownRequested, 0, 0) == TRUE)
	{
		IOCPCore::CloseSocketHandle(acceptSession->DetachSocket());

		if (!acceptSession->OnDisconnect())
		{
			__debugbreak();
		}

		acceptSession->DecrementIO();
		acceptSession->ResetSession();

		return;
	}

	ClientSession* clientSession = nullptr;

	if (!m_sessionManager->IsClientSessionFull())
	{
		// Client Session Pool 이 여유 있는 경우
		ISession* clientSessionBase = m_sessionManager->AcquireClientSession();
		clientSession = dynamic_cast<ClientSession*>(clientSessionBase);

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
				__debugbreak();
			}

			acceptSession->ResetSession();

			if (!PostAccept(acceptSession))
			{
				Logger::Log(LogLevel::LOG_ERROR, "[%s] 1 Dummy Session Accept 실패!", __FUNCTION__);

				//Log::log(LogLevel::LOG_ERROR, "[%s] Failed to post accept\n", __FUNCTION__);
				return;
			}
		}
		else
		{
			// Client Session 획득에 성공한 경우
			// socket 을 붙이고 IOCP 에 등록 후 PostRecv 하러 간다.

			sockaddr_in* localAddr = nullptr;
			sockaddr_in* remoteAddr = nullptr;
			int localLen = 0, remoteLen = 0;
			int addrSize = sizeof(sockaddr_in) + 16;

			char strIPAddress[INET_ADDRSTRLEN] = { 0, };
			uint16_t port = 0;

			if (acceptSession->GetAcceptBuffer())
			{
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
					// 네트워크 바이트 순서의 주소를 문자열로 변환
					if (::inet_ntop(AF_INET, &remoteAddr->sin_addr, strIPAddress, sizeof(strIPAddress)))
					{
						port = ::ntohs(remoteAddr->sin_port);

						Logger::Log(LogLevel::LOG_INFO, "[%s][%d] Client Connected - IP: %s, Port: %d",
							__FUNCTION__, clientSession->GetSessionID(), strIPAddress, port);
					}
				}
			}


			SOCKET acceptedSocket = acceptSession->DetachSocket();
			acceptSession->ResetSession();

			if (clientSession->GetClientSocket() != INVALID_SOCKET || clientSession->GetServerSessionState() != ServerSessionState::CONNECT_READY)
			{
				__debugbreak();
			}

			Logger::Log(LogLevel::LOG_INFO, "[%s][Listen Sesision : %d] socket(%d) 를 ClientSession(%d) 에 Attach", __FUNCTION__, session->GetSessionID(), (int)acceptedSocket, clientSession->GetSessionID());

			clientSession->AttachSocket(acceptedSocket);
			clientSession->SetRemoteAddress(strIPAddress, port);

			// Session 과 1:1 대응하는 User 를 초기화 한다.
			// Session 은 네트워크 담당, User 는 서비스 로직을 담당한다.
			//User* pUser = m_userManager->GetUser(clientSession->GetSessionID());
			//pUser->ResetUser();
			//if (!pUser->Initialize(clientSession, clientSession->GetSessionID()))
			//{
			//	__debugbreak();
			//}
			//clientSession->SetUser(pUser);

			if (!PostAccept(acceptSession))
			{
				Logger::Log(LogLevel::LOG_ERROR, "[%s] 2 Dummy Session Accept 실패!", __FUNCTION__);

				//Log::log(LogLevel::LOG_ERROR, "[%s] Failed to post accept\n", __FUNCTION__);
				return;
			}
		}
	}
	else
	{
		// Client Session Pool 이 여유 없는 경우
		// 해당 연결을 강제 종료 처리 후 Accept 를 걸어 준다.

		Logger::Log(LogLevel::LOG_INFO, "[%s][Listen Sesision : %d] Client Session Pool 에 비어 있는 Session 이 없어서 Disconnect 처리됨!", __FUNCTION__, session->GetSessionID());

		IOCPCore::CloseSocketHandle(acceptSession->DetachSocket());

		if (!acceptSession->OnDisconnect())
		{
			__debugbreak();
		}

		acceptSession->ResetSession();

		if (!PostAccept(acceptSession))
		{
			Logger::Log(LogLevel::LOG_ERROR, "[%s] 3 Dummy Session Accept 실패!", __FUNCTION__);

			//Log::log(LogLevel::LOG_ERROR, "[%s] Failed to post accept\n", __FUNCTION__);
			return;
		}
		return;
	}

	if (clientSession == nullptr)
	{
		__debugbreak();
		return;
	}

	Logger::Log(LogLevel::LOG_INFO, "[%s][%d] 해당 세션 서버 연결 시퀀스 수행 진행", __FUNCTION__, clientSession->GetSessionID());

	if (::InterlockedCompareExchange(&m_serverShutdownRequested, 0, 0) == TRUE)
	{
		// 이미 서버가 Shutdown 모드면 이 Accept 결과는 버린다.
		//m_sessionManager->DecrementAcceptCount();

		// 세션을 다시 풀로 반환
		m_sessionManager->ReleaseClientSession(clientSession);

		return;
	}

	if (!SocketOption::SetAcceptContext(clientSession->GetClientSocket(), m_serverSocket))
	{
		Logger::Log(LogLevel::LOG_ERROR, "[%s][ClientSession : %d] SetAcceptContext 실패", __FUNCTION__, clientSession->GetSessionID());
		m_sessionManager->ReleaseClientSession(clientSession);
		return;
	}

	if (!SocketOption::SetNoDelay(clientSession->GetClientSocket()))
	{
		Logger::Log(LogLevel::LOG_ERROR, "[%s][ClientSession : %d] SetNoDelay 실패", __FUNCTION__, clientSession->GetSessionID());
		m_sessionManager->ReleaseClientSession(clientSession);
		return;
	}

	if (!SocketOption::SetKeepAliveEx(clientSession->GetClientSocket(), 10'000, 1'000))
	{
		Logger::Log(LogLevel::LOG_ERROR, "[%s][ClientSession : %d] SetKeepAliveEx 실패", __FUNCTION__, clientSession->GetSessionID());
		m_sessionManager->ReleaseClientSession(clientSession);
		return;
	}
	//SocketOption::SetLinger(clientsocket, false); // 소켓 종료 즉시(선택)

	if (!IOCPCore::RegisterSocketToIOCP((ULONG_PTR)clientSession, clientSession->GetClientSocket()))
	{
		Logger::Log(LogLevel::LOG_ERROR, "[%s][ClientSession : %d] Client socket IOCP 등록 실패", __FUNCTION__, clientSession->GetSessionID());
		m_sessionManager->ReleaseClientSession(clientSession);
		return;
	}

	if (!clientSession->OnConnect())
	{
		m_sessionManager->ReleaseClientSession(clientSession);
		return;
	}
	OnClientConnect(clientSession);

	//Log::log(LogLevel::LOG_INFO, "[%s] New connection accepted. socket: %d\n", __FUNCTION__, session->getclientsocket());
}

void IOCPServer::HandleAcceptIOCancelled(uint32_t sessionId)
{
	Logger::Log(LogLevel::LOG_WARNING, "[%s] Listen Session Accept IO Canceled", __FUNCTION__);

	ISession* session = m_sessionManager->GetAcceptSession(sessionId);

	AcceptSession* acceptSession = dynamic_cast<AcceptSession*>(session);

	if (!acceptSession)
	{
		Logger::Log(LogLevel::LOG_ERROR, "[%s] Listen Session is nullptr", __FUNCTION__);

		return;
	}

	acceptSession->SetAcceptSessionState(AcceptSessionState::ACCEPT_ABORTED);
	acceptSession->DecrementIO();
}

void IOCPServer::HandleRecv(OverlappedEx* overlappedEx, ISession* session, DWORD bytesTransferred)
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
				m_sessionManager->ReleaseClientSession(clientSession);
				return;
			}

			MEMORY_POOL::ReleasePacket(*GetPacketMemoryPool(), *GetGeneralMemoryPool(), packetDataByMemoryPool);
			continue;
		}

		if (!clientSession->IsEstablished())
		{
			MEMORY_POOL::ReleasePacket(*GetPacketMemoryPool(), *GetGeneralMemoryPool(), packetDataByMemoryPool);
			Logger::Log(LogLevel::LOG_WARNING, "[%s][ClientSession : %d] service packet received before session established (PacketID : %u)", __FUNCTION__, clientSession->GetSessionID(), packetId);
			m_sessionManager->ReleaseClientSession(clientSession);
			return;
		}

		// 패킷 완성! 실제 처리 호출
		OnReceive(clientSession, packetId, packetDataByMemoryPool, packetSize);
	}

	// 다시 다음 수신 요청
	bool bResult = clientSession->PostReceive();
}

void IOCPServer::HandleRecvCancelled(OverlappedEx* overlappedEx, ISession* session)
{
	if (!session)
		return;

	if (!overlappedEx)
		return;

	session->DecrementIO();

	printf_s("[IOCPServer] :: [Session ID : %d] Recv Cancelled.\n", session->GetSessionID());
}

void IOCPServer::HandleSend(OverlappedEx* overlappedEx, ISession* session, DWORD bytesTransferred)
{
	if (!session)
		return;

	if (!overlappedEx)
		return;

	ClientSession* clientSession = dynamic_cast<ClientSession*>(session);

	if (clientSession->GetSessionRole() != SESSION_ROLE::SERVER)
	{
		Logger::Log(LogLevel::LOG_ERROR, "[%s] CLIENT Session 이 아닌 세션 ID\n", __FUNCTION__);

		__debugbreak();
		return;
	}

	clientSession->DecrementIO();

	bool bResult = clientSession->OnSendCompleted(bytesTransferred);
}

void IOCPServer::HandleSendCancelled(OverlappedEx* overlappedEx, ISession* session)
{
	if (!session)
		return;

	if (!overlappedEx)
		return;

	session->DecrementIO();

	printf_s("[IOCPServer] :: [Session ID : %d] Send Cancelled.\n", session->GetSessionID());
}

void IOCPServer::HandleSessionDisconnected(OverlappedEx* overlappedEx, ISession* session, DWORD bytesTransferred)
{
	// 세션의 정상 종료 시퀀스

	if (session == nullptr || overlappedEx == nullptr)
	{
		__debugbreak();
		return;
	}

	if (overlappedEx->operation == IO_OPERATION::RECV)
	{
		// 서비스 로직에게 세션 끊김을 먼저 알린다.
		OnClientDisconnect(session);

		// RECV I/O 걸려있던 세션이였으므로 IO 수량을 하나 빼주어야 한다.
		session->DecrementIO();

		// Session 과 User 를 디커플링한다.
		//User* pUser = m_userManager->GetUser(session->GetSessionID());
		//pUser->ResetUser();

		m_sessionManager->ReleaseClientSession(session); // I/O 를 모두 취소하고 취소되기를 기다린다.

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

bool IOCPServer::CreateListenSocket(const char* ipAddress, uint16_t port)
{
	m_serverSocket = ::WSASocket(AF_INET, SOCK_STREAM, IPPROTO_TCP, NULL, 0, WSA_FLAG_OVERLAPPED);

	if (m_serverSocket == INVALID_SOCKET)
	{
		//Log::log(LogLevel::LOG_ERROR, "[%s] WSASocket Listen socket failed\n", __FUNCTION__);
		return false;
	}


	//Log::log(LogLevel::LOG_INFO, "[%s] WSASocket Listen socket success\n", __FUNCTION__);

	// Listen 소켓도 IOCP에 연결
	if (CreateIoCompletionPort((HANDLE)m_serverSocket, GetIOCPHandle(), (ULONG_PTR)m_serverSocket, 0) == NULL)
	{
		//Log::log(LogLevel::LOG_ERROR, "[%s] CreateIoCompletionPort failed for listen socket - ErroCode : %d\n", __FUNCTION__, WSAGetLastError());

		return false;
	}

	//Log::log(LogLevel::LOG_INFO, "[%s] CreateIoCompletionPort success for listen socket\n", __FUNCTION__);

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
			Logger::Log(LogLevel::LOG_ERROR, "[%s] bind 에 사용할 IP 주소가 잘못됨: %s", __FUNCTION__, ipAddress);
			return false;
		}
	}

	if (::bind(serverSocket, (SOCKADDR*)&serverAddr, sizeof(serverAddr)) == SOCKET_ERROR)
	{
		//Log::log(LogLevel::LOG_ERROR, "[%s] bind failed - ErroCode : %d\n", __FUNCTION__, WSAGetLastError());

		return false;
	}

	//Log::log(LogLevel::LOG_INFO, "[%s] bind success\n", __FUNCTION__);

	return true;
}

bool IOCPServer::ListenServerSocket(SOCKET serverSocket)
{
	if (::listen(serverSocket, SOMAXCONN) == SOCKET_ERROR)
	{
		//Log::log(LogLevel::LOG_ERROR, "[%s] listen failed - ErroCode : %d\n", __FUNCTION__, WSAGetLastError());

		return false;
	}

	//Log::log(LogLevel::LOG_INFO, "[%s] listen success\n", __FUNCTION__);

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
		//Log::log(LogLevel::LOG_ERROR, "[%s] WSAIoctl Get AcceptEX Pointer Failed - ErroCode : %d\n", __FUNCTION__, WSAGetLastError());

		return false;
	}

	// GetAcceptExSockaddrs 함수 포인터 얻기
	GUID GuidGetAddrs = WSAID_GETACCEPTEXSOCKADDRS;
	if (::WSAIoctl(serverSocket, SIO_GET_EXTENSION_FUNCTION_POINTER,
		&GuidGetAddrs, sizeof(GuidGetAddrs),
		&m_getAcceptExSockAddrs, sizeof(m_getAcceptExSockAddrs),
		&bytes, nullptr, nullptr) == SOCKET_ERROR)
	{
		//Log::log(LogLevel::LOG_ERROR, "[%s] WSAIoctl Get GetAcceptExSockaddrs Pointer Failed - ErroCode : %d\n", __FUNCTION__, WSAGetLastError());

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
		ISession* session = m_sessionManager->GetAcceptSession(i);

		if (!session)
		{
			Logger::Log(LogLevel::LOG_ERROR, "[%s] Accept Session 획득 실패\n", __FUNCTION__);
			__debugbreak();
			return false;
		}

		if (session->GetSessionRole() != SESSION_ROLE::ACCEPT)
		{
			Logger::Log(LogLevel::LOG_ERROR, "[%s] Accept Session 이 아닌 세션 ID\n", __FUNCTION__);

			__debugbreak();
			return false;
		}

		if (session->GetAcceptSessionState() != AcceptSessionState::ACCEPT_READY)
		{
			Logger::Log(LogLevel::LOG_ERROR, "[%s] Accept Session 이 Accept 대기 중이 아님\n", __FUNCTION__);
			__debugbreak();
			return false;
		}

		if (!PostAccept(session))
		{
			Logger::Log(LogLevel::LOG_ERROR, "[%s] Accept Session AcceptEx 호출 실패\n", __FUNCTION__);
			__debugbreak();
			return false;
		}
	}

	return true;
}

bool IOCPServer::PrepareAccept(uint32_t sessionId)
{
	ISession* session = m_sessionManager->GetAcceptSession(sessionId);

	if (!session)
	{
		Logger::Log(LogLevel::LOG_ERROR, "[%s] Accept Session 획득 실패\n", __FUNCTION__);

		__debugbreak();
		return false;
	}

	if (session->GetSessionRole() != SESSION_ROLE::ACCEPT)
	{
		Logger::Log(LogLevel::LOG_ERROR, "[%s] Accept Session 이 아닌 세션 ID\n", __FUNCTION__);

		__debugbreak();
		return false;
	}

	if (session->GetAcceptSessionState() != AcceptSessionState::ACCEPT_READY)
	{
		Logger::Log(LogLevel::LOG_ERROR, "[%s] Accept Session 이 Accept 대기 중이 아님\n", __FUNCTION__);
		__debugbreak();
		return false;
	}

	if (!PostAccept(session))
	{
		Logger::Log(LogLevel::LOG_ERROR, "[%s] Accept Session AcceptEx 호출 실패\n", __FUNCTION__);

		return false;
	}

	return true;
}

bool IOCPServer::PostAccept(ISession* session)
{
	if (!session)
	{
		Logger::Log(LogLevel::LOG_ERROR, "[%s] AcceptEx 시작하려고 세션을 캐스팅 했는데 nullptr", __FUNCTION__);

		return false;
	}

	AcceptSession* acceptSession = dynamic_cast<AcceptSession*>(session);

	if (!acceptSession)
	{
		Logger::Log(LogLevel::LOG_ERROR, "[%s][Listen Sesision : %d] AcceptEx 시작하려고 세션을 캐스팅 했는데 nullptr", __FUNCTION__, acceptSession->GetSessionID());

		return false;
	}

	Logger::Log(LogLevel::LOG_INFO, "[%s][Listen Sesision : %d] AcceptEx 시작", __FUNCTION__, session->GetSessionID());

	if (::InterlockedCompareExchange(&m_serverShutdownRequested, 0, 0) == TRUE)
	{
		// 이미 서버가 Shutdown 모드면 이 Accept 결과는 버린다.

		IOCPCore::CloseSocketHandle(acceptSession->DetachSocket());

		if (!acceptSession->OnDisconnect())
		{
			__debugbreak();
		}

		acceptSession->ResetSession();

		return false;
	}

	constexpr int maxImmediateRetryCount = 3;
	for (int attempt = 0; attempt < maxImmediateRetryCount; ++attempt)
	{
		// 클라이언트와 연결될 소켓 생성
		SOCKET clientSocket = ::WSASocket(AF_INET, SOCK_STREAM, 0, NULL, 0, WSA_FLAG_OVERLAPPED);
		if (clientSocket == INVALID_SOCKET)
		{
			Logger::Log(LogLevel::LOG_ERROR, "[%s][Listen Sesision : %d] AcceptEx 시작하려고 WSASocket 호출하였는데 소켓이 INVALID", __FUNCTION__, acceptSession->GetSessionID());

			return false;
		}

		Logger::Log(LogLevel::LOG_INFO, "[%s][Listen Sesision : %d] socket 생성(%d)", __FUNCTION__, session->GetSessionID(), (int)clientSocket);

		// 세션에 클라이언트 소켓을 등록
		acceptSession->SetClientSocket(clientSocket);

		// IO 수량 증가(AcceptEx 에 대한 IO)
		acceptSession->IncrementIO();

		// Overlapped 구조체 초기화
		OverlappedEx& overlappedEx = acceptSession->GetAcceptOverlapped();  // session이 미리 생성한 OverlappedEx 포인터 반환

		overlappedEx.wsaBuffer.len = 0;
		overlappedEx.wsaBuffer.buf = nullptr;
		overlappedEx.operation = IO_OPERATION::ACCEPT;
		overlappedEx.sessionId = acceptSession->GetSessionID();

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
					Logger::Log(LogLevel::LOG_WARNING, "[%s][Listen Sesision : %d] AcceptEx 도중 클라이언트 연결이 끊김", __FUNCTION__, acceptSession->GetSessionID());
				}
				else if (nError == WSAENOBUFS || nError == WSAEMFILE)
				{
					Logger::Log(LogLevel::LOG_ERROR, "[%s][Listen Sesision : %d] AcceptEx 시스템 리소스 부족", __FUNCTION__, acceptSession->GetSessionID());
					::Sleep(10);
				}
				else if (nError == ERROR_OPERATION_ABORTED)
				{
					Logger::Log(LogLevel::LOG_WARNING, "[%s][Listen Sesision : %d] AcceptEx Aborted", __FUNCTION__, acceptSession->GetSessionID());
					return false;
				}
				else
				{
					Logger::Log(LogLevel::LOG_ERROR, "[%s][Listen Sesision : %d] AcceptEx 실패 (ERROR CODE : %d)", __FUNCTION__, acceptSession->GetSessionID(), nError);
				}

				continue;
			}
		}

		return true;
	}

	return false;
}

ReadySessionQueue* IOCPServer::GetReadySessionQueue() const
{
	return m_readySessionQueue;
}

PacketHandlerTable* IOCPServer::GetPacketHandlerTable() const
{
	return m_packetHandlerTable;
}

SlabMemoryPool* IOCPServer::GetJobMemoryPool() const
{
	return m_jobMemoryPool;
}

SlabMemoryPool* IOCPServer::GetPacketMemoryPool() const
{
	return m_packetMemoryPool;
}

SlabMemoryPool* IOCPServer::GetGeneralMemoryPool() const
{
	return m_generalMemoryPool;
}

const HandlerContext& IOCPServer::GetHandlerContext() const
{
	return m_handlerContext;
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

bool IOCPServer::HandleSystemPacket(ClientSession* session, uint16_t packetId, const char* packetData, uint32_t packetSize)
{
	if (!session || !packetData)
	{
		return false;
	}

	switch (static_cast<PACKET_ID>(packetId))
	{
	case PACKET_ID::CS_SYSTEM_AUTH_REQUEST:
	{
		if (packetSize != sizeof(CS_SYSTEM_AUTH_REQUEST_PACKET))
		{
			return false;
		}

		const CS_SYSTEM_AUTH_REQUEST_PACKET* request = reinterpret_cast<const CS_SYSTEM_AUTH_REQUEST_PACKET*>(packetData);
		if (session->GetServerSessionState() != ServerSessionState::CONNECTED &&
			session->GetServerSessionState() != ServerSessionState::AUTH_PENDING)
		{
			return SendSystemAuthResponse(session, SYSTEM_AUTH_RESULT::INVALID_STATE);
		}

		if (request->protocolVersion != IOCP_ENGINE_PROTOCOL_VERSION)
		{
			return SendSystemAuthResponse(session, SYSTEM_AUTH_RESULT::PROTOCOL_MISMATCH);
		}

		session->SetServerSessionState(ServerSessionState::AUTH_PENDING);

		if (!SendSystemAuthResponse(session, SYSTEM_AUTH_RESULT::SUCCESS))
		{
			return false;
		}

		session->SetServerSessionState(ServerSessionState::ESTABLISHED);
		Logger::Log(LogLevel::LOG_INFO, "[%s][ClientSession : %d] session established", __FUNCTION__, session->GetSessionID());
		return true;
	}

	case PACKET_ID::CS_SYSTEM_HEARTBEAT_RESPONSE:
	{
		if (packetSize != sizeof(CS_SYSTEM_HEARTBEAT_RESPONSE_PACKET))
		{
			return false;
		}

		if (!session->IsEstablished())
		{
			return false;
		}

		session->UpdateLastHeartbeatTick();
		return true;
	}

	default:
		Logger::Log(LogLevel::LOG_WARNING, "[%s][ClientSession : %d] unhandled system packet id: %u", __FUNCTION__, session->GetSessionID(), packetId);
		return false;
	}
}
