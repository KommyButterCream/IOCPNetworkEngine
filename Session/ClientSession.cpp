#include "ClientSession.h"

#include <WinSock2.h>
#include <ws2ipdef.h> // for INET_ADDRSTRLEN

#include "SessionJobQueue.h"
#include "ISessionEvent.h"

#include "../Buffer/RecvPacketBuffer.h"
#include "../Buffer/SendPacketQueue.h"
#include "../Buffer/SendPacketPool.h"
#include "../Buffer/HybridSendPacketPool.h"
#include "../Memory/SlabMemoryPoolHelper.h"
#include "../../CoreLib/Utils/Logger.h"

#include "../Scheduler/ClientSessionScheduler.h"
#include "../Job/Job.h"
#include "../Protocol/PacketHeader.h"
#include "../Protocol/PacketID.h"
#include "../Protocol/SystemPacket.h"

#pragma comment(lib, "ws2_32.lib") // for WinSock2

using namespace CoreLibrary::Utils;

ClientSession::ClientSession()
{
	::ZeroMemory(&m_connectOverlapped, sizeof(OverlappedEx));
	m_connectOverlapped.operation = IO_OPERATION::CONNECT;

	::ZeroMemory(&m_recvOverlapped, sizeof(OverlappedEx));
	m_recvOverlapped.operation = IO_OPERATION::RECV;

	::ZeroMemory(&m_sendOverlapped, sizeof(OverlappedEx));
	m_sendOverlapped.operation = IO_OPERATION::SEND;
}

ClientSession::~ClientSession()
{
	Finalize();
}

bool ClientSession::Initialize(SESSION_ROLE sessionType, uint32_t sessionId)
{
	if (!BaseSession::Initialize(sessionType, sessionId))
		return false;

	if (GetSessionRole() == SESSION_ROLE::CLIENT)
	{
		SetClientSessionState(ClientSessionState::CONNECT_READY);
	}
	else if (GetSessionRole() == SESSION_ROLE::SERVER)
	{
		SetServerSessionState(ServerSessionState::CONNECT_READY);
	}

	return true;
}

void ClientSession::ResetSession()
{
	BaseSession::ResetSession();

	if (GetSessionRole() == SESSION_ROLE::CLIENT)
	{
		SetClientSessionState(ClientSessionState::CONNECT_READY);
	}
	else if (GetSessionRole() == SESSION_ROLE::SERVER)
	{
		SetServerSessionState(ServerSessionState::CONNECT_READY);
	}

	::InterlockedExchange(&m_sending, 0);
	::InterlockedExchange(&m_processing, 0);

	if (m_sessionContext)
	{
		delete m_sessionContext;
		m_sessionContext = nullptr;
	}

	if (m_sendPacketQueue)
	{
		m_sendPacketQueue->Reset();
	}
	m_currentSendPacket = nullptr;
	m_sendOffset = 0;

	if (m_jobQueue)
	{
		m_jobQueue->Reset();
	}

	m_lastRecvBufferFullTime = 0;
	::InterlockedExchange64(&m_lastRecvTick, 0);
	::InterlockedExchange64(&m_lastHeartbeatTick, 0);

	if (GetSessionRole() == SESSION_ROLE::CLIENT)
	{
		::ZeroMemory(&m_connectOverlapped, sizeof(OverlappedEx));
		m_connectOverlapped.operation = IO_OPERATION::CONNECT;
	}
	else if (GetSessionRole() == SESSION_ROLE::SERVER)
	{
		::ZeroMemory(&m_recvOverlapped, sizeof(OverlappedEx));
		m_recvOverlapped.operation = IO_OPERATION::RECV;

		::ZeroMemory(&m_sendOverlapped, sizeof(OverlappedEx));
		m_sendOverlapped.operation = IO_OPERATION::SEND;
	}
}

void ClientSession::Finalize()
{
	if (m_destroyFlag)
	{
		return;
	}

	BaseSession::Finalize();

	ClearRemoteAddress();

	if (m_sessionContext)
	{
		delete m_sessionContext;
		m_sessionContext = nullptr;
	}

	m_eventHandler = nullptr;

	if (m_sendPacketQueue)
	{
		m_sendPacketQueue->Finalize();
		delete m_sendPacketQueue;
		m_sendPacketQueue = nullptr;
	}

	m_currentSendPacket = nullptr;
	m_sendOffset = 0;

	if (m_jobQueue)
	{
		m_jobQueue->Reset();
		delete m_jobQueue;
		m_jobQueue = nullptr;
	}

	if (m_recvPacketBuffer)
	{
		m_recvPacketBuffer->Finalize();
		delete m_recvPacketBuffer;
		m_recvPacketBuffer = nullptr;
	}

	m_sendPacketPool = nullptr;

	if (GetSessionRole() == SESSION_ROLE::CLIENT)
	{
		SetClientSessionState(ClientSessionState::CONNECT_READY);
	}
	else if (GetSessionRole() == SESSION_ROLE::SERVER)
	{
		SetServerSessionState(ServerSessionState::CONNECT_READY);
	}

	::InterlockedExchange(&m_sending, 0);
	::InterlockedExchange(&m_processing, 0);

	m_lastRecvBufferFullTime = 0;
	::InterlockedExchange64(&m_lastRecvTick, 0);
	::InterlockedExchange64(&m_lastHeartbeatTick, 0);

	if (GetSessionRole() == SESSION_ROLE::CLIENT)
	{
		::ZeroMemory(&m_connectOverlapped, sizeof(OverlappedEx));
		m_connectOverlapped.operation = IO_OPERATION::CONNECT;
	}
	else if (GetSessionRole() == SESSION_ROLE::SERVER)
	{
		::ZeroMemory(&m_recvOverlapped, sizeof(OverlappedEx));
		m_recvOverlapped.operation = IO_OPERATION::RECV;

		::ZeroMemory(&m_sendOverlapped, sizeof(OverlappedEx));
		m_sendOverlapped.operation = IO_OPERATION::SEND;
	}

	m_packetMemoryPool = nullptr;
	m_generalMemoryPool = nullptr;
	m_jobMemoryPool = nullptr;

	m_destroyFlag = true;

}

bool ClientSession::OnAccept()
{
	return false;
}

bool ClientSession::OnConnect()
{
	if (GetSessionRole() == SESSION_ROLE::CLIENT)
	{
		SetClientSessionState(ClientSessionState::CONNECTED);
	}
	else if (GetSessionRole() == SESSION_ROLE::SERVER)
	{
		SetServerSessionState(ServerSessionState::CONNECTED);
	}

	Logger::Log(LogLevel::LOG_INFO, "[%s][ClientSession : %d] 클라이언트 Accept 완료", __FUNCTION__, GetSessionID());

	Logger::Log(LogLevel::LOG_HIGH, "[%s][ClientSession : %d] IP : %s, Port : %d", __FUNCTION__, GetSessionID(), m_clientIPAddress, m_clientPort);
	UpdateLastRecvTick();
	UpdateLastHeartbeatTick();

	if (!PostReceive())
	{
		Logger::Log(LogLevel::LOG_ERROR, "[%s][ClientSession : %d] 클라이언트 Accept 후 WSARecv 실패", __FUNCTION__, GetSessionID());

		return false;
	}

	return true;
}

bool ClientSession::OnDisconnect()
{
	ClearRemoteAddress();

	if (::InterlockedExchange(&m_closing, 1) == 0)
	{
		Logger::Log(LogLevel::LOG_INFO, "[%s][ClientSession : %d] CloseSocket(%d) 시작", __FUNCTION__, GetSessionID(), (int)GetClientSocket());

		if (!IsSocketInvalid())
		{
			Logger::Log(LogLevel::LOG_WARNING, "[%s][ClientSession : %d] 소켓이 정리되지 않은 상태에서 Disconnect 처리됨 (Socket : %d)", __FUNCTION__, GetSessionID(), static_cast<int>(GetClientSocket()));
			__debugbreak();
			return false;
		}

		if (GetSessionRole() == SESSION_ROLE::CLIENT)
			SetClientSessionState(ClientSessionState::DISCONNECTED);
		else if (GetSessionRole() == SESSION_ROLE::SERVER)
			SetServerSessionState(ServerSessionState::DISCONNECTED);
	}

	return true;
}

bool ClientSession::InitializeMemoryPool(HybridSendPacketPool* hybridSendPacketPool, SlabMemoryPool* jobMemoryPool, SlabMemoryPool* packetMemoryPool, SlabMemoryPool* generalMemoryPool)
{
	m_jobMemoryPool = jobMemoryPool;
	m_packetMemoryPool = packetMemoryPool;
	m_generalMemoryPool = generalMemoryPool;

	m_recvPacketBuffer = new RecvPacketBuffer();
	if (!m_recvPacketBuffer)
		return false;
	if (!m_recvPacketBuffer->Initialize(packetMemoryPool))
	{
		delete m_recvPacketBuffer;
		m_recvPacketBuffer = nullptr;
		return false;
	}

	m_jobQueue = new SessionJobQueue(GetSessionRole(), jobMemoryPool, packetMemoryPool, generalMemoryPool);
	if (!m_jobQueue)
	{
		m_recvPacketBuffer->Finalize();
		delete m_recvPacketBuffer;
		m_recvPacketBuffer = nullptr;
		return false;
	}

	if (!hybridSendPacketPool)
	{
		delete m_jobQueue;
		m_jobQueue = nullptr;
		m_recvPacketBuffer->Finalize();
		delete m_recvPacketBuffer;
		m_recvPacketBuffer = nullptr;
		return false;
	}

	m_sendPacketPool = hybridSendPacketPool->GetPool(GetSessionID());
	if (!m_sendPacketPool)
	{
		delete m_jobQueue;
		m_jobQueue = nullptr;
		m_recvPacketBuffer->Finalize();
		delete m_recvPacketBuffer;
		m_recvPacketBuffer = nullptr;
		return false;
	}

	m_sendPacketQueue = new SendPacketQueue();
	if (!m_sendPacketQueue)
	{
		delete m_jobQueue;
		m_jobQueue = nullptr;
		m_recvPacketBuffer->Finalize();
		delete m_recvPacketBuffer;
		m_recvPacketBuffer = nullptr;
		m_sendPacketPool = nullptr;
		return false;
	}

	if (!m_sendPacketQueue->Initialize(m_sendPacketPool, packetMemoryPool, generalMemoryPool))
	{
		delete m_sendPacketQueue;
		m_sendPacketQueue = nullptr;
		delete m_jobQueue;
		m_jobQueue = nullptr;
		m_recvPacketBuffer->Finalize();
		delete m_recvPacketBuffer;
		m_recvPacketBuffer = nullptr;
		m_sendPacketPool = nullptr;
		return false;
	}

	return true;
}

bool ClientSession::IsReady() const
{
	if (GetSessionRole() == SESSION_ROLE::CLIENT)
		return (GetClientSessionState() == ClientSessionState::CONNECT_READY);

	if (GetSessionRole() == SESSION_ROLE::SERVER)
		return (GetServerSessionState() == ServerSessionState::CONNECT_READY);

	return false;
}

bool ClientSession::IsConnected() const
{
	if (GetSessionRole() == SESSION_ROLE::CLIENT)
		return (GetClientSessionState() == ClientSessionState::CONNECTED || GetClientSessionState() == ClientSessionState::AUTH_PENDING || GetClientSessionState() == ClientSessionState::ESTABLISHED);

	if (GetSessionRole() == SESSION_ROLE::SERVER)
		return (GetServerSessionState() == ServerSessionState::CONNECTED || GetServerSessionState() == ServerSessionState::AUTH_PENDING || GetServerSessionState() == ServerSessionState::ESTABLISHED || GetServerSessionState() == ServerSessionState::HEARTBEAT_TIMEOUT);

	return false;
}

bool ClientSession::IsEstablished() const
{
	if (GetSessionRole() == SESSION_ROLE::CLIENT)
		return (GetClientSessionState() == ClientSessionState::ESTABLISHED);

	if (GetSessionRole() == SESSION_ROLE::SERVER)
		return (GetServerSessionState() == ServerSessionState::ESTABLISHED);

	return false;
}

bool ClientSession::IsTransportConnected() const
{
	return IsConnected();
}

OverlappedEx& ClientSession::GetConnectOverlapped()
{
	return m_connectOverlapped;
}

const RecvPacketBuffer& ClientSession::GetReceiveBuffer() const
{
	return *m_recvPacketBuffer;
}

RecvPacketBuffer& ClientSession::GetReceiveBuffer()
{
	return *m_recvPacketBuffer;
}

const SendPacketQueue* ClientSession::GetSendPacketQueue() const
{
	return m_sendPacketQueue;
}

SendPacketQueue* ClientSession::GetSendPacketQueue()
{
	return m_sendPacketQueue;
}

SessionJobQueue& ClientSession::GetJobQueue() const
{
	return *m_jobQueue;
}

bool ClientSession::PostReceive()
{
	Logger::Log(LogLevel::LOG_INFO, "[%s][ClientSession : %d] Receive 시작", __FUNCTION__, GetSessionID());

	if (!IsTransportConnected())
	{
		Logger::Log(LogLevel::LOG_ERROR, "[%s][ClientSession : %d] 세션이 연결 중이 아님", __FUNCTION__, GetSessionID());

		return false;
	}

	DWORD flags = 0;
	DWORD bytesReceived = 0;

	RecvPacketBuffer& recvBuf = GetReceiveBuffer();

	m_recvOverlapped.operation = IO_OPERATION::RECV;
	m_recvOverlapped.wsaBuffer.buf = recvBuf.GetWriteablePtr();
	m_recvOverlapped.wsaBuffer.len = recvBuf.GetWriteableSize();

	if (m_recvOverlapped.wsaBuffer.len == 0)
	{
		Logger::Log(LogLevel::LOG_ERROR, "[%s][ClientSession : %d] Recv 를 위한 버퍼가 꽉 참", __FUNCTION__, GetSessionID());

		// 버퍼에 공간이 부족한 경우 시간을 저장했다가
		// 별도의 타이머 스레드에서 타임아웃 관련 처리(Session Disconnect 등) 하도록 한다.
		if (m_lastRecvBufferFullTime == 0)
		{
			m_lastRecvBufferFullTime = ::GetTickCount64();
		}

		//Log::log(LogLevel::LOG_WARNING, "[%s] Buffer space is not enough to send data.\n", __FUNCTION__);

		__debugbreak();

		return false;
	}

	m_lastRecvBufferFullTime = 0;

	IncrementIO();

	int result = ::WSARecv(
		GetClientSocket(),
		&m_recvOverlapped.wsaBuffer,
		1,
		&bytesReceived,
		&flags,
		(LPWSAOVERLAPPED)&m_recvOverlapped,
		nullptr);

	if (result == SOCKET_ERROR)
	{
		int errorCode = ::WSAGetLastError();
		if (errorCode != WSA_IO_PENDING)
		{
			Logger::Log(LogLevel::LOG_ERROR, "[%s][ClientSession : %d] WSARecv 실패(ERROR CODE : %d)", __FUNCTION__, GetSessionID(), errorCode);

			// 더이상 해당 세션에 I/O 를 걸 수 없는 상태이므로 중요한 예외 처리 부분이다!
			HandleSocketError(errorCode, IO_OPERATION::RECV);

			return false;
		}
	}

	//Log::log(LogLevel::LOG_INFO, "[%s] WSARecv async success : %d\n", __FUNCTION__, clientSocket_);

	return true;
}

bool ClientSession::TrySendNext()
{
	if (GetSendPacketQueue() == nullptr)
		return false;

	// 이미 전송 중이라면 WSASend 호출 하지 않고 빠져나간다.
	if (::InterlockedCompareExchange(&m_sending, 1, 0) != 0)
	{
		return false;
	}

	if (!m_currentSendPacket)
	{
		if (!GetSendPacketQueue()->Dequeue(m_currentSendPacket))
		{
			// 보낼 패킷이 없는 경우
			// Idle 상태로 플래그를 다시 변경 해준다.
			::InterlockedExchange(&m_sending, 0);
			return false;
		}

		m_sendOffset = 0;
	}

	if (!PostCurrentSend())
	{
		::InterlockedExchange(&m_sending, 0);
		return false;
	}

	return true;
}

bool ClientSession::PostCurrentSend()
{
	if (!m_currentSendPacket)
		return false;

	const DWORD remainingBytes = m_currentSendPacket->packetSize - m_sendOffset;

	if (remainingBytes == 0)
	{
		// PacketData 반환
		MEMORY_POOL::ReleasePacket(*m_packetMemoryPool, *m_generalMemoryPool, m_currentSendPacket->packetData);

		m_sendPacketPool->Release(m_currentSendPacket);
		m_currentSendPacket = nullptr;
		::InterlockedExchange(&m_sending, 0);

		return false;
	}

	::ZeroMemory(&m_sendOverlapped, sizeof(OverlappedEx));
	m_sendOverlapped.operation = IO_OPERATION::SEND;
	m_sendOverlapped.sessionId = GetSessionID();
	m_sendOverlapped.wsaBuffer.buf = const_cast<char*>(m_currentSendPacket->packetData + m_sendOffset);
	m_sendOverlapped.wsaBuffer.len = remainingBytes;

	DWORD flags = 0;
	DWORD bytesSent = 0;

	IncrementIO();

	int result = ::WSASend(
		GetClientSocket(),
		&m_sendOverlapped.wsaBuffer,
		1,
		&bytesSent,
		flags,
		(LPWSAOVERLAPPED)&m_sendOverlapped,
		nullptr);

	if (result == SOCKET_ERROR)
	{
		int errorCode = WSAGetLastError();
		if (errorCode != WSA_IO_PENDING)
		{
			Logger::Log(LogLevel::LOG_ERROR, "[%s][ClientSession : %d] WSASend 실패(ERROR CODE : %d)", __FUNCTION__, GetSessionID(), errorCode);

			// 더이상 해당 세션에 I/O 를 걸 수 없는 상태이므로 중요한 예외 처리 부분이다!
			HandleSocketError(errorCode, IO_OPERATION::SEND);

			return false;
		}
	}

	return true;
}

bool ClientSession::OnSendCompleted(const DWORD bytesTransferred)
{
	Logger::Log(LogLevel::LOG_INFO, "[%s][ClientSession : %d] Send 완료", __FUNCTION__, GetSessionID());

	// 부분 전송 처리
	m_sendOffset += bytesTransferred;

	SendPacketBuffer* currentPacket = m_currentSendPacket;
	if (!currentPacket)
	{
		::InterlockedExchange(&m_sending, 0);
		return false;
	}

	// 아직 덜 보낸 데이터가 남았다면 이어서 송신
	if (m_sendOffset < currentPacket->packetSize)
	{
		return PostCurrentSend();
	}

	// 완전 전송 완료
	// PacketData 반환
	MEMORY_POOL::ReleasePacket(*m_packetMemoryPool, *m_generalMemoryPool, currentPacket->packetData);

	m_sendPacketPool->Release(currentPacket);
	m_currentSendPacket = nullptr;

	m_sendOffset = 0;
	::InterlockedExchange(&m_sending, 0);

	// 다음 패킷 있으면 이어서 송신
	TrySendNext();

	return true;
}

bool ClientSession::EnqueueJob(Job* job, bool& wasEmpty)
{
	if (job == nullptr)
		return false;

	// 세션 JobQueue에 enqueue
	if (!GetJobQueue().EnqueueJob(job, wasEmpty))
	{
		Logger::Log(LogLevel::LOG_ERROR, "[%s][ClientSession : %d] Failed to enqueue Job for sessionD", __FUNCTION__, GetSessionID());

		__debugbreak();

		return false;
	}

	return true;
}

bool ClientSession::SubmitJob(Job* job)
{
	if (!job || !m_jobMemoryPool || !m_packetMemoryPool || !m_generalMemoryPool)
		return false;

	bool wasEmpty = false;
	if (!EnqueueJob(job, wasEmpty))
	{
		MEMORY_POOL::ReleasePacket(*m_packetMemoryPool, *m_generalMemoryPool, job->data);
		MEMORY_POOL::ReleaseJob(*m_jobMemoryPool, job);
		return false;
	}

	return true;
}

bool ClientSession::EnqueueSendPacket(void** packetData, uint32_t packetSize)
{
	if (packetData == nullptr || packetSize <= 0 || GetSendPacketQueue() == nullptr)
		return false;

	const PACKET_HEADER* packetHeader = reinterpret_cast<const PACKET_HEADER*>(*packetData);
	if (packetHeader == nullptr || packetSize < sizeof(PACKET_HEADER))
	{
		Logger::Log(LogLevel::LOG_ERROR, "[%s][ClientSession : %d] invalid packet header", __FUNCTION__, GetSessionID());
		return false;
	}

	if (packetHeader->packetSize != packetSize)
	{
		Logger::Log(LogLevel::LOG_ERROR, "[%s][ClientSession : %d] packet size mismatch (header=%u, arg=%u)", __FUNCTION__, GetSessionID(), packetHeader->packetSize, packetSize);
		return false;
	}

	if (!CanSendPacket(packetHeader->packetId))
	{
		Logger::Log(LogLevel::LOG_WARNING, "[%s][ClientSession : %d] packet send blocked before session established (PacketID : %u)", __FUNCTION__, GetSessionID(), packetHeader->packetId);
		return false;
	}

	uint32_t retryCount = 0;
	while (true)
	{
		if (GetSendPacketQueue()->Enqueue(packetData, packetSize))
		{
			// SendQueue 에 Enqueue 성공한 경우 while 탈출

			TrySendNext();

			//if (retryCount > 0)
			//{
			//	printf_s("Retry Count : %d\n", retryCount);
			//}

			return true;
		}

		retryCount++;
		::Sleep(0);

		if (retryCount > 100)
		{
			Logger::Log(LogLevel::LOG_ERROR, "[%s][ClientSession : %d] Failed to enqueue SendPacket(Retry : %d)", __FUNCTION__, GetSessionID(), retryCount);
		}
	}

	return false;
}

void ClientSession::HandleSocketError(int errorCode, IO_OPERATION ioOperation)
{
	Logger::Log(LogLevel::LOG_INFO, "[%s][ClientSession : %d][IO : %d] 에러 핸들링", __FUNCTION__, GetSessionID(), (int)ioOperation);

	// 공용으로 처리 되어야 하는 예외 처리
	// WSASend 호출 이전에 증가시켰던 Send/Recv IO Count 복구
	DecrementIO();

	// IO Operation Type 에 따른 우선 처리 되어야 하는 예외 처리
	if (ioOperation == IO_OPERATION::RECV)
	{

	}
	else if (ioOperation == IO_OPERATION::SEND)
	{
		// SEND 인 경우
		// 현재 처리되어야 하는 패킷을 패킷풀에 반환하고
		// nullptr 초기화 한다.
		if (m_currentSendPacket)
		{
			MEMORY_POOL::ReleasePacket(*m_packetMemoryPool, *m_generalMemoryPool, m_currentSendPacket->packetData);
			m_sendPacketPool->Release(m_currentSendPacket);
			m_currentSendPacket = nullptr;
		}

		m_sendOffset = 0;
		::InterlockedExchange(&m_sending, 0);
	}
	switch (errorCode)
	{
	case WSAECONNRESET:       // 연결이 비정상 종료됨 (상대방 강제 종료)
	case WSAECONNABORTED:     // 연결 중단됨
	case WSAENOTCONN:         // 연결이 이미 끊김
	case WSAESHUTDOWN:        // 소켓 송수신 불가
	case ERROR_NETNAME_DELETED: // 네트워크 이름 삭제됨 (연결 끊김)
		//Log::Info("[Session %u] Connection closed (op=%d, err=%d)", m_sessionId, opType, err);
		//OnDisconnected();
		NotifyDisconnect();
		break;

	case WSAENOBUFS:
	case WSAEMFILE:
		//Log::Warn("[Session %u] System resource shortage (op=%d, err=%d)", m_sessionId, opType, err);
		::Sleep(10); // 잠시 대기 후 재시도 가능 (너무 과도한 재시도는 주의)
		break;

	case ERROR_OPERATION_ABORTED:
		// CancelIoEx 또는 서버 종료 중 발생한 정상적인 취소
		//Log::Debug("[Session %u] Operation canceled (op=%d)", m_sessionId, opType);
		break;

	default:
		NotifyDisconnect();
		//Log::Error("[Session %u] Unknown socket error %d (op=%d)", m_sessionId, err, opType);
		break;
	}
}

void ClientSession::SetEventHandler(ISessionEvent* handler)
{
	m_eventHandler = handler;
}

void ClientSession::NotifyDisconnect()
{
	if (m_eventHandler)
	{
		m_eventHandler->OnDisconnectRequest(this);
	}
}

void ClientSession::UpdateProcessingFlag(LONG value)
{
	::InterlockedExchange(&m_processing, value);
}

bool ClientSession::IsProcessingReady()
{
	// 0 : 준비 상태
	// 1 : 프로세싱 중인 상태
	return (::InterlockedCompareExchange(&m_processing, 1, 0) == 0);
}

void ClientSession::SetSessionContext(SessionContext* sessionContext)
{
	if (m_sessionContext != nullptr && m_sessionContext != sessionContext)
	{
		delete m_sessionContext;
	}

	m_sessionContext = sessionContext;
}

SessionContext* ClientSession::GetSessionContext() const
{
	return m_sessionContext;
}

void ClientSession::SetCurrentJob(Job* job)
{
	// 스케쥴러가 처리 중인 Job 을 세션이 알도록 설정
	// 필요에 따라 ClearCurrentJobData 로 Job 에 Link 되어 있는
	// PacketData 에 대한 소유권을 포기하기 위해 작업 전에 설정.
	m_currentJob = job;
}

void ClientSession::ClearCurrentJobData()
{
	// 현재 Send 중인 패킷 데이터에 대한 소유권 포기.
	// Send IO Complete 시점에 패킷 메모리 해제함.
	m_currentJob->data = nullptr;
}

void ClientSession::SetRemoteAddress(const char* ipAddress, uint16_t port)
{
	if (ipAddress == nullptr)
		return;

	memcpy_s(m_clientIPAddress, sizeof(m_clientIPAddress), ipAddress, INET_ADDRSTRLEN);
	m_clientPort = port;
}

void ClientSession::ClearRemoteAddress()
{
	memset(m_clientIPAddress, 0, sizeof(m_clientIPAddress));
	m_clientPort = 0;
}

void ClientSession::UpdateLastRecvTick()
{
	::InterlockedExchange64(&m_lastRecvTick, static_cast<LONGLONG>(::GetTickCount64()));
}

void ClientSession::UpdateLastHeartbeatTick()
{
	::InterlockedExchange64(&m_lastHeartbeatTick, static_cast<LONGLONG>(::GetTickCount64()));
}

uint64_t ClientSession::GetLastRecvTick() const
{
	return static_cast<uint64_t>(::InterlockedCompareExchange64(const_cast<volatile LONGLONG*>(&m_lastRecvTick), 0, 0));
}

uint64_t ClientSession::GetLastHeartbeatTick() const
{
	return static_cast<uint64_t>(::InterlockedCompareExchange64(const_cast<volatile LONGLONG*>(&m_lastHeartbeatTick), 0, 0));
}

uint64_t ClientSession::GetLastActiveTick() const
{
	const uint64_t lastRecvTick = GetLastRecvTick();
	const uint64_t lastHeartbeatTick = GetLastHeartbeatTick();
	return (lastRecvTick > lastHeartbeatTick) ? lastRecvTick : lastHeartbeatTick;
}

bool ClientSession::IsHeartbeatTimedOut(uint64_t nowTick, uint64_t timeout_ms) const
{
	if (timeout_ms == 0)
	{
		return false;
	}

	const uint64_t lastActiveTick = GetLastActiveTick();
	if (lastActiveTick == 0 || nowTick < lastActiveTick)
	{
		return false;
	}

	return ((nowTick - lastActiveTick) >= timeout_ms);
}

void ClientSession::MarkHeartbeatTimeout()
{
	if (GetSessionRole() == SESSION_ROLE::SERVER)
	{
		SetServerSessionState(ServerSessionState::HEARTBEAT_TIMEOUT);
	}
}

bool ClientSession::SendSystemHeartbeatRequest()
{
	if (GetSessionRole() != SESSION_ROLE::SERVER || !IsEstablished())
	{
		return false;
	}

	void* packetMemory = MEMORY_POOL::CreatePacket(*m_packetMemoryPool, sizeof(SC_SYSTEM_HEARTBEAT_REQUEST_PACKET));
	if (!packetMemory)
	{
		return false;
	}

	SC_SYSTEM_HEARTBEAT_REQUEST_PACKET* requestPacket = reinterpret_cast<SC_SYSTEM_HEARTBEAT_REQUEST_PACKET*>(packetMemory);
	*requestPacket = SC_SYSTEM_HEARTBEAT_REQUEST_PACKET();
	requestPacket->tick = ::GetTickCount64();

	void* packetData = requestPacket;
	if (!EnqueueSendPacket(&packetData, sizeof(SC_SYSTEM_HEARTBEAT_REQUEST_PACKET)))
	{
		MEMORY_POOL::ReleasePacket(*m_packetMemoryPool, *m_generalMemoryPool, requestPacket);
		return false;
	}

	return true;
}

bool ClientSession::SendSystemHeartbeatResponse(uint64_t requestTick)
{
	if (GetSessionRole() != SESSION_ROLE::CLIENT || !IsEstablished())
	{
		return false;
	}

	void* packetMemory = MEMORY_POOL::CreatePacket(*m_packetMemoryPool, sizeof(CS_SYSTEM_HEARTBEAT_RESPONSE_PACKET));
	if (!packetMemory)
	{
		return false;
	}

	CS_SYSTEM_HEARTBEAT_RESPONSE_PACKET* responsePacket = reinterpret_cast<CS_SYSTEM_HEARTBEAT_RESPONSE_PACKET*>(packetMemory);
	*responsePacket = CS_SYSTEM_HEARTBEAT_RESPONSE_PACKET();
	responsePacket->tick = requestTick;

	void* packetData = responsePacket;
	if (!EnqueueSendPacket(&packetData, sizeof(CS_SYSTEM_HEARTBEAT_RESPONSE_PACKET)))
	{
		MEMORY_POOL::ReleasePacket(*m_packetMemoryPool, *m_generalMemoryPool, responsePacket);
		return false;
	}

	return true;
}

bool ClientSession::CanSendPacket(PACKET_ID_TYPE packetId) const
{
	if (IsSystemPacketId(packetId))
	{
		return IsTransportConnected();
	}

	if (IsServicePacketId(packetId))
	{
		return IsEstablished();
	}

	return false;
}




