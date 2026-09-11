#include "ClientSession.h"

#include <WinSock2.h>

#include "../Diagnostics/EngineAssert.h"
#include <ws2ipdef.h> // for INET_ADDRSTRLEN

#include "SessionJobQueue.h"
#include "ISessionEvent.h"

#include "../Buffer/RecvPacketBuffer.h"
#include "../Buffer/SendPacketQueue.h"
#include "../Memory/EngineMemoryPoolHelper.h"
#include "../../Core/Util/Logger.h"

#include "../Scheduler/ClientSessionScheduler.h"
#include "../Job/Job.h"
#include "../Protocol/PacketHeader.h"
#include "../Protocol/PacketID.h"
#include "../Protocol/SystemPacket.h"

#pragma comment(lib, "ws2_32.lib") // for WinSock2

using namespace Core::Util;

namespace
{
	// 상대가 연결을 끊어서 발생한 I/O 실패인지 판정한다.
	// 이건 정상적인 종료 경로이므로 ERROR 로 올리면 진짜 문제가 묻힌다.
	bool IsPeerClosedError(int errorCode)
	{
		switch (errorCode)
		{
		case WSAECONNRESET:         // 상대가 강제 종료 (RST)
		case WSAECONNABORTED:       // 연결 중단
		case WSAENOTCONN:           // 이미 끊김
		case WSAESHUTDOWN:          // 송수신 불가
		case ERROR_NETNAME_DELETED: // 네트워크 이름 삭제 = 연결 끊김
		case ERROR_OPERATION_ABORTED: // CancelIoEx 로 인한 정상 취소
			return true;
		default:
			return false;
		}
	}
}

ClientSession::ClientSession()
{
	m_connectOverlapped.operation = IO_OPERATION::CONNECT;
	m_recvOverlapped.operation = IO_OPERATION::RECV;
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

	// 래치가 남아 있으면 다음 접속이 이 세션을 쓰다가, 접속을 알린 적도
	// 없는데 종료 통지를 받는다. 정상 경로에서는 반납 직전에 이미
	// 소비되지만 통지 배선이 없는 구성도 있어 여기서 확실히 지운다.
	::InterlockedExchange(&m_serviceConnectNotified, 0);

	if (m_sessionContext)
	{
		delete m_sessionContext;
		m_sessionContext = nullptr;
	}

	if (m_sendPacketQueue)
	{
		m_sendPacketQueue->Reset();
	}

	// 수신 링을 비운다. 이게 없으면 세션이 끊길 때 링에 남아 있던 바이트가
	// 그대로 살아남아, 이 슬롯을 재사용하는 다음 클라이언트의 스트림 앞에
	// 붙는다. 그 클라이언트는 첫 바이트부터 남의 데이터를 헤더로 읽는다.
	// (RecvPacketBuffer::Reset 은 지금까지 아무도 부르지 않고 있었다)
	//
	// GetReceiveBuffer 는 널 검사 없이 역참조하는데, ResetSession 은 정리
	// 경로에서도 불리므로 포인터로 직접 다룬다.
	if (m_recvPacketBuffer)
	{
		m_recvPacketBuffer->Reset();
	}

	// 전송 중이던 엔트리를 반환한다.
	// 포인터만 버리면 패킷 메모리와 엔트리가 함께 누수된다.
	// (RST 등으로 전송 도중에 세션이 정리되는 경로에서 실제로 발생한다)
	if (m_currentSendPacket && m_sendPacketQueue)
	{
		m_sendPacketQueue->ReleaseEntry(m_currentSendPacket);
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
		m_connectOverlapped.Clear();
	}
	else if (GetSessionRole() == SESSION_ROLE::SERVER)
	{
		m_recvOverlapped.Clear();
		m_sendOverlapped.Clear();
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

	// 전송 중이던 엔트리를 반환한다. (ResetSession 과 같은 이유)
	// 반드시 ReleaseMemoryResources 보다 먼저 해야 한다. 그쪽이 송신 큐를
	// 지우고, 엔트리를 되돌릴 수 있는 것은 그 큐뿐이다.
	if (m_currentSendPacket && m_sendPacketQueue)
	{
		m_sendPacketQueue->ReleaseEntry(m_currentSendPacket);
	}
	m_currentSendPacket = nullptr;
	m_sendOffset = 0;

	// 송신 큐 / 잡 큐 / 수신 링.
	// InitializeMemoryPool 의 실패 정리와 같은 코드를 쓴다.
	ReleaseMemoryResources();

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
		m_connectOverlapped.Clear();
	}
	else if (GetSessionRole() == SESSION_ROLE::SERVER)
	{
		m_recvOverlapped.Clear();
		m_sendOverlapped.Clear();
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

	// 접속 1건에 로그 2줄을 쓰던 것을 한 줄로 합친다.
	// 원격 주소는 서버 역할에서만 채워진다 (SetRemoteAddress 는 HandleAccept 가 호출).
	// 클라이언트 역할이면 비어 있으므로 찍지 않는다.
	if (m_clientIPAddress[0] != '\0')
	{
		LOGI("session %u connected from %s:%u (role %d)",
			GetSessionID(), m_clientIPAddress, m_clientPort, static_cast<int>(GetSessionRole()));
	}
	else
	{
		LOGI("session %u connected (role %d)", GetSessionID(), static_cast<int>(GetSessionRole()));
	}

	UpdateLastRecvTick();
	UpdateLastHeartbeatTick();

	if (!PostReceive())
	{
		// 첫 수신을 걸지 못하면 이 세션은 아무것도 받을 수 없다.
		LOGE("session %u failed to post the first recv right after connect", GetSessionID());
		return false;
	}

	return true;
}

bool ClientSession::OnDisconnect()
{
	ClearRemoteAddress();
	return BaseSession::OnDisconnect();
}

bool ClientSession::InitializeMemoryPool(EngineMemoryPool* sendQueueMemoryPool, EngineMemoryPool* jobMemoryPool, EngineMemoryPool* packetMemoryPool, EngineMemoryPool* generalMemoryPool, const SessionBufferConfig& bufferConfig)
{
	if (!bufferConfig.IsValid())
	{
		ENGINE_VIOLATION("session %u received an invalid buffer config (recv %u/ring %u, send %u, queue %u)",
			GetSessionID(), bufferConfig.maxRecvPacketSize, bufferConfig.recvRingSize,
			bufferConfig.maxSendPacketSize, bufferConfig.sendQueueDepth);
		return false;
	}

	// 널 풀은 아무것도 잡기 전에 걸러낸다. 예전에는 이 검사가 세 번째
	// 단계에 있어서, 실패가 확정된 뒤에 링 버퍼와 잡 큐를 만들었다 지웠다.
	if (!sendQueueMemoryPool)
	{
		ENGINE_VIOLATION("session %u received a null send queue memory pool", GetSessionID());
		return false;
	}

	m_bufferConfig = bufferConfig;

	m_jobMemoryPool = jobMemoryPool;
	m_packetMemoryPool = packetMemoryPool;
	m_generalMemoryPool = generalMemoryPool;

	// 아래 어느 단계에서 실패하든 정리는 ReleaseMemoryResources 한 곳에서 한다.
	//
	// 예전에는 단계마다 되감기 코드를 계단식으로 복제했다. 다섯 벌이 조금씩
	// 달랐고, 그중 하나는 m_sendPacketQueue 를 Finalize 없이 delete 해서
	// 큐가 들고 있던 것을 반환하지 않았다. 단계를 하나 추가하면 여섯 번째
	// 사본이 늘어나는 구조였다.

	m_recvPacketBuffer = new RecvPacketBuffer();
	if (!m_recvPacketBuffer ||
		!m_recvPacketBuffer->Initialize(packetMemoryPool, m_bufferConfig.recvRingSize, m_bufferConfig.maxRecvPacketSize))
	{
		ReleaseMemoryResources();
		return false;
	}

	m_jobQueue = new SessionJobQueue(GetSessionRole(), jobMemoryPool, packetMemoryPool, generalMemoryPool);
	if (!m_jobQueue)
	{
		ReleaseMemoryResources();
		return false;
	}

	m_sendPacketQueue = new SendPacketQueue();
	if (!m_sendPacketQueue ||
		!m_sendPacketQueue->Initialize(sendQueueMemoryPool, packetMemoryPool, generalMemoryPool, m_bufferConfig.sendQueueDepth))
	{
		ReleaseMemoryResources();
		return false;
	}

	return true;
}

// InitializeMemoryPool 이 잡은 것만 되돌린다.
// 실패 정리와 Finalize 가 같은 코드를 쓰게 하려고 뺐다.
//
// 순서는 Finalize 가 쓰던 것을 그대로 따른다.
//
// 부분 생성 상태에서 불려도 안전하다. 세 자원의 Finalize/Reset 은 전부
// 널 검사로 시작한다.
void ClientSession::ReleaseMemoryResources()
{
	if (m_sendPacketQueue)
	{
		m_sendPacketQueue->Finalize();
		delete m_sendPacketQueue;
		m_sendPacketQueue = nullptr;
	}

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
}

bool ClientSession::IsReady() const
{
	if (GetSessionRole() == SESSION_ROLE::CLIENT)
		return (GetClientSessionState() == ClientSessionState::CONNECT_READY);

	if (GetSessionRole() == SESSION_ROLE::SERVER)
		return (GetServerSessionState() == ServerSessionState::CONNECT_READY);

	return false;
}

// 소켓이 붙어 있는가. 인증 여부는 보지 않는다.
//
// IsEstablished(인증까지 끝났는가) 와 짝을 이루는 두 단계 중 아래쪽이다.
// 시스템 패킷(인증 요청, 하트비트)은 인증 전에도 오가야 하므로 이쪽을 보고,
// 서비스 패킷은 IsEstablished 를 본다. CanSendPacket 이 그 분기다.
//
// 예전에는 이 함수의 별칭인 IsConnected 가 따로 있었다. 두 이름이 같은 것을
// 계산하니 독자는 없는 구분을 찾게 되고, 한쪽만 고치면 조용히 갈라진다.
bool ClientSession::IsTransportConnected() const
{
	// 상태를 지역 변수에 한 번만 받는다.
	//
	// 예전에는 비교마다 getter 를 다시 불렀다. 상태가 평범한 멤버이던
	// 시절에도 옳지 않았고(다른 스레드가 그 사이에 바꾼다) 지금은 호출마다
	// 실제로 다시 읽으므로 분명한 결함이다. 비교 도중에 값이 바뀌면
	// 어느 항목에도 걸리지 않아, 실제로는 접속되어 있는 세션이
	// "접속 아님" 으로 판정될 수 있다.
	if (GetSessionRole() == SESSION_ROLE::CLIENT)
	{
		const ClientSessionState state = GetClientSessionState();
		return (state == ClientSessionState::CONNECTED
			|| state == ClientSessionState::AUTH_PENDING
			|| state == ClientSessionState::ESTABLISHED);
	}

	if (GetSessionRole() == SESSION_ROLE::SERVER)
	{
		const ServerSessionState state = GetServerSessionState();
		return (state == ServerSessionState::CONNECTED
			|| state == ServerSessionState::AUTH_PENDING
			|| state == ServerSessionState::ESTABLISHED
			|| state == ServerSessionState::HEARTBEAT_TIMEOUT);
	}

	return false;
}

void ClientSession::MarkServiceConnectNotified()
{
	::InterlockedExchange(&m_serviceConnectNotified, 1);
}

bool ClientSession::ConsumeServiceConnectNotified()
{
	return (::InterlockedExchange(&m_serviceConnectNotified, 0) == 1);
}

bool ClientSession::IsEstablished() const
{
	if (GetSessionRole() == SESSION_ROLE::CLIENT)
		return (GetClientSessionState() == ClientSessionState::ESTABLISHED);

	if (GetSessionRole() == SESSION_ROLE::SERVER)
		return (GetServerSessionState() == ServerSessionState::ESTABLISHED);

	return false;
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
	// 수신 1건당 호출되는 핫패스. 릴리스에서는 컴파일 제거된다.
	LOGT("session %u post recv", GetSessionID());

	if (!IsTransportConnected())
	{
		LOGE("session %u cannot post recv : the session is not connected", GetSessionID());

		return false;
	}

	DWORD flags = 0;
	DWORD bytesReceived = 0;

	RecvPacketBuffer& recvBuf = GetReceiveBuffer();

	// 링 끝에 붙어 있으면 남은 조각을 앞으로 당겨 연속 공간을 만든다.
	// 이게 없으면 쓰기 위치가 끝에 가까워질수록 WSARecv 길이가 줄어들어,
	// 같은 양을 받는 데 필요한 완료 횟수가 계속 늘어난다.
	recvBuf.PrepareWrite();

	m_recvOverlapped.ResetForNextIO(GetSessionID(),	recvBuf.GetWriteablePtr(), recvBuf.GetWriteableSize());

	if (m_recvOverlapped.wsaBuffer.len == 0)
	{
		LOGE("session %u recv ring is full, cannot post recv (stored %u / capacity %u)", GetSessionID(), recvBuf.GetStoredSize(), recvBuf.GetCapacity());

		// 버퍼에 공간이 부족한 경우 시간을 저장했다가
		// 별도의 타이머 스레드에서 타임아웃 관련 처리(Session Disconnect 등) 하도록 한다.
		if (m_lastRecvBufferFullTime == 0)
		{
			m_lastRecvBufferFullTime = ::GetTickCount64();
		}

		ENGINE_BREAK_IF_DEBUGGER();

		return false;
	}

	m_lastRecvBufferFullTime = 0;

	// 취소가 걸린 뒤에는 새 수신을 걸지 않는다. (사정은 BaseSession::BeginIO 주석)
	//
	// 아이디를 미리 읽어 둔다. BeginIO 가 false 를 돌려주면 그 안의 되돌림이
	// 마지막 카운트였을 수 있고, 그러면 이 세션은 이미 풀로 돌아가 있다.
	const uint32_t sessionIdForLog = GetSessionID();

	if (!BeginIO())
	{
		LOGI("session %u declining to post recv : IO cancel is in progress", sessionIdForLog);
		return false;
	}

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
			if (IsPeerClosedError(errorCode))
				LOGI("session %u recv ended : the peer closed the connection (error %d)", GetSessionID(), errorCode);
			else
				LOGE("session %u WSARecv failed (error %d)", GetSessionID(), errorCode);

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

	for (;;)
	{
		// 이미 전송 중이라면 WSASend 호출 하지 않고 빠져나간다.
		// 전송을 담당 중인 스레드가 완료 시점에 다음 패킷을 이어서 보낸다.
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

				// 토큰을 반납하기 직전에 다른 스레드가 Enqueue 했을 수 있다.
				// 그 스레드는 m_sending 이 1 이어서 "전송 담당자가 이어서 보내줄 것"으로
				// 판단하고 물러났으므로, 여기서 재확인하지 않으면
				// 그 패킷은 큐에 갇힌 채 아무도 보내지 않는 상태가 된다.
				if (GetSendPacketQueue()->IsEmpty())
				{
					return false;
				}

				// 그 사이에 들어온 패킷이 있으므로 토큰 재획득을 시도한다.
				continue;
			}

			m_sendOffset = 0;
		}

		// PostCurrentSend 는 실패 시 내부에서(또는 HandleSocketError 에서)
		// 현재 패킷과 m_sending 토큰을 이미 정리한다.
		// 여기서 토큰을 한 번 더 반납하면, 그 사이 정당하게 토큰을 획득한
		// 다른 스레드의 소유권을 뺏어 WSASend 가 동시에 두 번 발행될 수 있다.
		// m_sendOverlapped 는 세션당 하나뿐이므로 진행 중인 I/O 구조체가 손상된다.
		return PostCurrentSend();
	}
}

bool ClientSession::PostCurrentSend()
{
	if (!m_currentSendPacket)
		return false;

	const DWORD remainingBytes = m_currentSendPacket->packetSize - m_sendOffset;

	if (remainingBytes == 0)
	{
		// 패킷 + 엔트리 반환
		m_sendPacketQueue->ReleaseEntry(m_currentSendPacket);
		m_currentSendPacket = nullptr;
		::InterlockedExchange(&m_sending, 0);

		return false;
	}

	m_sendOverlapped.ResetForNextIO(GetSessionID(),	const_cast<char*>(m_currentSendPacket->packetData + m_sendOffset), remainingBytes);

	DWORD flags = 0;
	DWORD bytesSent = 0;

	// 취소가 걸린 뒤에는 새 송신을 걸지 않는다. (사정은 BaseSession::BeginIO 주석)
	//
	// 정리를 BeginIO 앞에서 끝내는 것이 중요하다. 카운트를 들고 있는 동안은
	// 세션이 반납되지 않으므로 여기서 엔트리를 되돌리는 것이 안전하고,
	// BeginIO 가 false 를 돌려준 뒤에는 세션을 만질 수 없다.
	//
	// 엔트리를 되돌리지 않으면 패킷과 엔트리가 그대로 새고, m_sending
	// 토큰이 잡힌 채로 남아 이 세션은 이후 어떤 송신도 발행하지 못한다.
	const uint32_t sessionIdForLog = GetSessionID();

	if (IsIOCancelRequested())
	{
		LOGI("session %u declining to post send : IO cancel is in progress", sessionIdForLog);

		m_sendPacketQueue->ReleaseEntry(m_currentSendPacket);
		m_currentSendPacket = nullptr;
		m_sendOffset = 0;
		::InterlockedExchange(&m_sending, 0);

		return false;
	}

	if (!BeginIO())
	{
		// 위 검사와 여기 사이에 취소가 걸렸다. 정리는 카운트를 들고 있는
		// BeginIO 안에서 할 수 없으므로, 발행만 포기하고 엔트리는 그대로 둔다.
		// 세션 반납 경로(ResetSession / Finalize)가 m_currentSendPacket 을
		// 회수한다.
		LOGI("session %u declining to post send : IO cancel raced the post", sessionIdForLog);
		return false;
	}

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
			if (IsPeerClosedError(errorCode))
				LOGI("session %u send ended : the peer closed the connection (error %d)", GetSessionID(), errorCode);
			else
				LOGE("session %u WSASend failed (error %d)", GetSessionID(), errorCode);

			// 더이상 해당 세션에 I/O 를 걸 수 없는 상태이므로 중요한 예외 처리 부분이다!
			HandleSocketError(errorCode, IO_OPERATION::SEND);

			return false;
		}
	}

	return true;
}

bool ClientSession::OnSendCompleted(const DWORD bytesTransferred)
{
	// 송신 완료 1건당 호출되는 핫패스. 릴리스에서는 컴파일 제거된다.
	LOGT("session %u send completed %lu bytes", GetSessionID(), bytesTransferred);

	// 부분 전송 처리
	m_sendOffset += bytesTransferred;

	SendPacketEntry* currentPacket = m_currentSendPacket;
	if (!currentPacket)
	{
		// 다른 경로(HandleSocketError 등)가 이미 패킷과 토큰을 정리한 상태다.
		// 여기서 m_sending 을 건드리면 그 사이 토큰을 획득한 다른 스레드의
		// 소유권을 뺏게 되므로 손대지 않는다.
		return false;
	}

	// 아직 덜 보낸 데이터가 남았다면 이어서 송신
	if (m_sendOffset < currentPacket->packetSize)
	{
		return PostCurrentSend();
	}

	// 완전 전송 완료
	// 패킷 + 엔트리 반환
	m_sendPacketQueue->ReleaseEntry(currentPacket);
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
		LOGE("session %u failed to enqueue a job", GetSessionID());

		ENGINE_BREAK_IF_DEBUGGER();

		return false;
	}

	return true;
}

// 잡을 이 세션의 큐에 올린다.
//
// 소유권
//   성공하면 job 과 job->data 는 큐의 것이다.
//   실패하면 둘 다 부르는 쪽에 그대로 남는다. 부르는 쪽이 반납해야 한다.
//
// 예전에는 이 함수가 실패 경로에서 직접 반납했다. 그런데 반납에 쓰는 풀이
// 세션의 멤버라, 초기화를 지나지 않은(또는 이미 정리된) 세션에서는 전부
// null 이었다. 그러면 맨 앞의 검사에 걸려 아무것도 반납하지 않은 채 false 만
// 돌아갔고, job 과 패킷이 조용히 샜다. 부르는 쪽은 계약상 소유권을 넘긴
// 것으로 알고 있으므로 아무도 회수하지 않는다.
//
// 그래서 반납을 그 메모리를 잡은 쪽으로 되돌린다. 부르는 쪽
// (IOCPClient::SubmitPacketJob)의 풀은 잡기 직전에 유효성이 확인되어 있어
// 항상 되돌릴 수 있다. 서버 쪽 SubmitPacketJob 이 원래 그 형태다.
bool ClientSession::SubmitJob(Job* job)
{
	if (!job)
		return false;

	// EnqueueJob 이 GetJobQueue() 를 역참조하는데 그쪽에는 null 검사가 없다.
	// 큐가 없는 세션에서는 곧바로 null 역참조가 되므로 여기서 막는다.
	//
	// 예전의 풀 검사가 이 시점을 우연히 같이 가리고 있었다 — 큐와 풀이
	// InitializeMemoryPool 에서 함께 설정되기 때문이다. 막아야 할 대상은
	// 큐이므로 검사도 큐를 본다.
	if (!m_jobQueue)
	{
		ENGINE_VIOLATION("session %u cannot submit a job : the session job queue is not initialized",
			GetSessionID());
		return false;
	}

	bool wasEmpty = false;
	return EnqueueJob(job, wasEmpty);
}

bool ClientSession::EnqueueSendPacket(void** packetData, uint32_t packetSize)
{
	if (packetData == nullptr || packetSize <= 0 || GetSendPacketQueue() == nullptr)
		return false;

	const PACKET_HEADER* packetHeader = reinterpret_cast<const PACKET_HEADER*>(*packetData);
	if (packetHeader == nullptr || packetSize < sizeof(PACKET_HEADER))
	{
		LOGE("session %u invalid packet header", GetSessionID());
		return false;
	}

	if (packetHeader->packetSize != packetSize)
	{
		LOGE("session %u packet size mismatch (header=%u, arg=%u)", GetSessionID(), packetHeader->packetSize, packetSize);
		return false;
	}

	// 받는 쪽은 자기 maxRecvPacketSize 를 넘는 헤더를 프로토콜 위반으로 보고
	// 연결을 끊는다. 여기서 막지 않으면 보낸 쪽은 원인 모를 피어 끊김만
	// 보게 되므로, 같은 상한을 송신 시점에 적용해 호출부에서 잡아준다.
	if (packetSize > m_bufferConfig.maxSendPacketSize)
	{
		LOGE("session %u packet too large to send (%u bytes, max %u). the peer would drop the connection",
			GetSessionID(), packetSize, m_bufferConfig.maxSendPacketSize);
		return false;
	}

	if (!CanSendPacket(packetHeader->packetId))
	{
		LOGW("session %u packet send blocked before session established (PacketID : %u)", GetSessionID(), packetHeader->packetId);
		return false;
	}

	if (!GetSendPacketQueue()->Enqueue(packetData, packetSize))
	{
		// 송신 큐가 가득 찬 상태.
		// 여기서 대기하면 호출 스레드(브로드캐스트 팬아웃 스레드, 로직 스레드 등)가
		// 느린 피어 하나 때문에 묶이므로, 즉시 실패를 반환하고
		// 드롭/재시도/연결 종료 같은 정책 판단은 호출자에게 맡긴다.
		//
		// 실패 시 Enqueue 는 *packetData 를 nullptr 로 만들지 않으므로
		// 패킷 메모리의 소유권은 호출자가 계속 보유한다. 호출자가 해제해야 한다.
		LOGW("session %u send queue full, packet dropped (PacketID : %u, Size : %u)", GetSessionID(), packetHeader->packetId, packetSize);
		return false;
	}

	TrySendNext();

	return true;
}

bool ClientSession::EnqueueSharedSendPacket(const void* packetData, uint32_t packetSize, SendPacketReleaseFunc releaseFunc, void* releaseContext)
{
	if (!packetData || packetSize == 0 || !releaseFunc || GetSendPacketQueue() == nullptr)
		return false;

	const PACKET_HEADER* packetHeader = reinterpret_cast<const PACKET_HEADER*>(packetData);
	if (packetHeader == nullptr || packetSize < sizeof(PACKET_HEADER))
	{
		LOGE("session %u invalid packet header", GetSessionID());
		return false;
	}

	if (packetHeader->packetSize != packetSize)
	{
		LOGE("session %u packet size mismatch (header=%u, arg=%u)", GetSessionID(), packetHeader->packetSize, packetSize);
		return false;
	}

	// 받는 쪽은 자기 maxRecvPacketSize 를 넘는 헤더를 프로토콜 위반으로 보고
	// 연결을 끊는다. 여기서 막지 않으면 보낸 쪽은 원인 모를 피어 끊김만
	// 보게 되므로, 같은 상한을 송신 시점에 적용해 호출부에서 잡아준다.
	if (packetSize > m_bufferConfig.maxSendPacketSize)
	{
		LOGE("session %u packet too large to send (%u bytes, max %u). the peer would drop the connection",
			GetSessionID(), packetSize, m_bufferConfig.maxSendPacketSize);
		return false;
	}

	if (!CanSendPacket(packetHeader->packetId))
	{
		LOGW("session %u packet send blocked before session established (PacketID : %u)", GetSessionID(), packetHeader->packetId);
		return false;
	}

	if (!GetSendPacketQueue()->EnqueueShared(packetData, packetSize, releaseFunc, releaseContext))
	{
		LOGW("session %u failed to enqueue shared SendPacket", GetSessionID());
		return false;
	}

	TrySendNext();

	return true;
}

void ClientSession::HandleSocketError(int errorCode, IO_OPERATION ioOperation)
{
	if (IsPeerClosedError(errorCode))
		LOGI("session %u io %d ended : the peer closed the connection (error %d)", GetSessionID(), (int)ioOperation, errorCode);
	else
		LOGE("session %u io %d socket error %d", GetSessionID(), (int)ioOperation, errorCode);

	// DecrementIO 는 이 함수 끝에서 한다.
	//
	// 여기서 복구하려는 카운트는 방금 실패한 WSARecv / WSASend 몫이다.
	// 예전에는 그걸 함수 맨 앞에서 내렸는데, 아래 정리와 NotifyDisconnect 가
	// 전부 그 뒤에 있었다. 지연 반납이 들어오면서 이 순서가 위험해졌다 —
	// 여기서 카운트가 0 이 되면 그 자리에서 세션이 반납되고 프리 리스트에
	// 올라가, 아래 코드가 이미 회수된(그리고 다른 접속에 재배포됐을 수도
	// 있는) 세션을 계속 만진다.
	//
	// 정리를 먼저 끝내고 마지막에 내려놓는다. NotifyDisconnect 는 이제
	// 기다리지 않고 예약만 하므로 이 순서가 성립한다.

	// IO Operation Type 에 따른 우선 처리 되어야 하는 예외 처리
	if (ioOperation == IO_OPERATION::RECV)
	{

	}
	else if (ioOperation == IO_OPERATION::SEND)
	{
		// SEND 인 경우
		// 현재 처리되어야 하는 엔트리를 큐에 반환하고
		// nullptr 초기화 한다.
		if (m_currentSendPacket && m_sendPacketQueue)
		{
			m_sendPacketQueue->ReleaseEntry(m_currentSendPacket);
		}
		m_currentSendPacket = nullptr;

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
		// 예전에는 여기서 ::Sleep(10) 을 했다. 이 함수는 IOCP 완료 핸들러
		// 안에서 불리므로 그 대기가 워커 스레드를 통째로 막는다. 게다가
		// 잠든 뒤 재시도하는 코드가 없어서 잠들기만 했다.
		//
		// 자원 부족은 이 세션 하나의 문제가 아니라 프로세스 전체의 문제다.
		// 세션 단위로 할 수 있는 일이 없으므로 크게 남기고 넘어간다.
		LOGE("session %u hit a system resource shortage (error %d, op %d)",
			GetSessionID(), errorCode, static_cast<int>(ioOperation));
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

	// 실패한 I/O 몫을 여기서 내려놓는다. 이 함수의 마지막 줄이어야 한다.
	// 위에서 NotifyDisconnect 가 반납을 예약했다면, 이 호출이 카운트를
	// 0 으로 내리는 순간 반납이 마무리된다. 그 뒤로 세션을 만지면 안 된다.
	DecrementIO();
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
	//
	// SetCurrentJob 이 널로 불린 뒤에 여기 오면 예전에는 즉시 크래시였다.
	// 이 클래스의 다른 함수들과 같이 널을 정상 입력으로 다룬다.
	if (!m_currentJob)
		return;

	m_currentJob->data = nullptr;
}

void ClientSession::SetRemoteAddress(const char* ipAddress, uint16_t port)
{
	if (ipAddress == nullptr)
		return;

	// 이전에는 memcpy_s(..., ipAddress, INET_ADDRSTRLEN) 로 소스에서 무조건
	// 22바이트를 읽었다. ipAddress 가 그보다 짧은 버퍼면 범위를 넘어 읽는다.
	// strncpy_s + _TRUNCATE 는 널 종료까지만 읽고, 대상 크기를 넘으면 자른다.
	::strncpy_s(m_clientIPAddress, sizeof(m_clientIPAddress), ipAddress, _TRUNCATE);
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




