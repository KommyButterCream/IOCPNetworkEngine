#include "ClientSession.h"

#include <WinSock2.h>

#include "../Diagnostics/EngineAssert.h"
#include <ws2ipdef.h> // for INET_ADDRSTRLEN

#include "SessionJobQueue.h"
#include "ISessionEvent.h"

#include "../Buffer/RecvPacketBuffer.h"
#include "../Buffer/SendPacketQueue.h"
#include "../Buffer/SendPacketPool.h"
#include "../Buffer/HybridSendPacketPool.h"
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
	void ReleaseSendPacketData(EngineMemoryPool& packetMemoryPool, EngineMemoryPool& generalMemoryPool, SendPacketBuffer* packetBuffer)
	{
		if (!packetBuffer || !packetBuffer->packetData)
			return;

		if (packetBuffer->releaseFunc)
		{
			packetBuffer->releaseFunc(packetBuffer->packetData, packetBuffer->releaseContext);
		}
		else
		{
			MEMORY_POOL::ReleasePacket(packetMemoryPool, generalMemoryPool, packetBuffer->packetData);
		}

		packetBuffer->Reset();
	}

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

	// 전송 중이던 패킷을 반환한다.
	// 포인터만 버리면 패킷 메모리와 SendPacketBuffer 가 함께 누수된다.
	// (RST 등으로 전송 도중에 세션이 정리되는 경로에서 실제로 발생한다)
	if (m_currentSendPacket)
	{
		if (m_packetMemoryPool && m_generalMemoryPool)
		{
			ReleaseSendPacketData(*m_packetMemoryPool, *m_generalMemoryPool, m_currentSendPacket);
		}

		if (m_sendPacketPool)
		{
			m_sendPacketPool->Release(m_currentSendPacket);
		}

		m_currentSendPacket = nullptr;
	}
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

	// 전송 중이던 패킷을 반환한다. (ResetSession 과 같은 이유)
	// 풀 포인터들은 이 함수 끝에서 nullptr 로 바뀌므로 그 전에 반환해야 한다.
	if (m_currentSendPacket)
	{
		if (m_packetMemoryPool && m_generalMemoryPool)
		{
			ReleaseSendPacketData(*m_packetMemoryPool, *m_generalMemoryPool, m_currentSendPacket);
		}

		if (m_sendPacketPool)
		{
			m_sendPacketPool->Release(m_currentSendPacket);
		}

		m_currentSendPacket = nullptr;
	}
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

	if (::InterlockedExchange(&m_closing, 1) == 0)
	{
		if (!IsSocketInvalid())
		{
			// 소켓은 반드시 DetachSocket 으로 먼저 떼어낸 뒤 여기 와야 한다.
			// 이전 메시지는 "CloseSocket(%d) 시작" 이었는데, 정상 경로에서는
			// 소켓이 이미 분리되어 항상 -1 이 찍혀 오히려 오해를 유발했다.
			LOGE("session %u disconnecting with a live socket %d. it was not detached",
				GetSessionID(), static_cast<int>(GetClientSocket()));
			ENGINE_BREAK_IF_DEBUGGER();
			return false;
		}

		if (GetSessionRole() == SESSION_ROLE::CLIENT)
			SetClientSessionState(ClientSessionState::DISCONNECTED);
		else if (GetSessionRole() == SESSION_ROLE::SERVER)
			SetServerSessionState(ServerSessionState::DISCONNECTED);
	}

	return true;
}

bool ClientSession::InitializeMemoryPool(HybridSendPacketPool* hybridSendPacketPool, EngineMemoryPool* jobMemoryPool, EngineMemoryPool* packetMemoryPool, EngineMemoryPool* generalMemoryPool, const SessionBufferConfig& bufferConfig)
{
	if (!bufferConfig.IsValid())
	{
		ENGINE_VIOLATION("session %u received an invalid buffer config (recv %u/ring %u, send %u, queue %u)",
			GetSessionID(), bufferConfig.maxRecvPacketSize, bufferConfig.recvRingSize,
			bufferConfig.maxSendPacketSize, bufferConfig.sendQueueDepth);
		return false;
	}

	m_bufferConfig = bufferConfig;

	m_jobMemoryPool = jobMemoryPool;
	m_packetMemoryPool = packetMemoryPool;
	m_generalMemoryPool = generalMemoryPool;

	m_recvPacketBuffer = new RecvPacketBuffer();
	if (!m_recvPacketBuffer)
		return false;
	if (!m_recvPacketBuffer->Initialize(packetMemoryPool, m_bufferConfig.recvRingSize, m_bufferConfig.maxRecvPacketSize))
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

	if (!m_sendPacketQueue->Initialize(m_sendPacketPool, packetMemoryPool, generalMemoryPool, m_bufferConfig.sendQueueDepth))
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

	m_recvOverlapped.operation = IO_OPERATION::RECV;
	m_recvOverlapped.wsaBuffer.buf = recvBuf.GetWriteablePtr();
	m_recvOverlapped.wsaBuffer.len = recvBuf.GetWriteableSize();

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
		// PacketData 반환
		ReleaseSendPacketData(*m_packetMemoryPool, *m_generalMemoryPool, m_currentSendPacket);

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

	SendPacketBuffer* currentPacket = m_currentSendPacket;
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
	// PacketData 반환
	ReleaseSendPacketData(*m_packetMemoryPool, *m_generalMemoryPool, currentPacket);

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
		LOGE("session %u failed to enqueue a job", GetSessionID());

		ENGINE_BREAK_IF_DEBUGGER();

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
			ReleaseSendPacketData(*m_packetMemoryPool, *m_generalMemoryPool, m_currentSendPacket);
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




