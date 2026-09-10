#include "ClientSessionPool.h"

#include "ClientSession.h"

#include "../Diagnostics/EngineAssert.h"
#include "SessionNode.h"

#include <string.h> // for strncmp

#include "../../Core/Sync/SRWLockGuard.h"
#include "../../Core/Util/Logger.h"

using namespace Core::Util;

ClientSessionPool::ClientSessionPool(uint32_t capacity, HybridSendPacketPool* hybridSendPacketPool, EngineMemoryPool* jobMemoryPool, EngineMemoryPool* packetMemoryPool, EngineMemoryPool* generalMemoryPool, const SessionBufferConfig& bufferConfig)
{
	m_capacity = capacity;

	m_sessions = new ClientSession[m_capacity];
	if (!m_sessions)
		return;

	m_nodes = new SessionNode[m_capacity];
	if (!m_nodes)
		return;

	// 초기 프리 리스트 구성
	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		if (!m_sessions[i].Initialize(SESSION_ROLE::SERVER, i))
		{
			ENGINE_VIOLATION("failed to initialize client session %u of %u", i, m_capacity);
			return;
		}

		if (!m_sessions[i].InitializeMemoryPool(hybridSendPacketPool, jobMemoryPool, packetMemoryPool, generalMemoryPool, bufferConfig))
		{
			ENGINE_VIOLATION("failed to bind memory pools to client session %u of %u", i, m_capacity);
			return;
		}

		m_nodes[i].session = &m_sessions[i];
		m_nodes[i].nextNode = m_freeList;
		m_freeList = &m_nodes[i];
	}

	m_ready = true;
}

ClientSessionPool::~ClientSessionPool()
{
	if (m_sessions)
	{
		delete[] m_sessions;
		m_sessions = nullptr;
	}

	if (m_nodes)
	{
		delete[] m_nodes;
		m_nodes = nullptr;
	}
}

ClientSession* ClientSessionPool::Acquire()
{
	Core::Sync::SRWWriteLockGuard lockguard(m_lock);

	if (!m_freeList)
	{
		return nullptr;
	}

	SessionNode* node = m_freeList;
	m_freeList = node->nextNode;
	node->nextNode = nullptr;

	// 프리 리스트에 올라와 있던 노드는 반드시 FREE 상태여야 한다.
	// 여기서 어긋나면 프리 리스트가 오염된 것이므로 임대하지 않고 실패시킨다.
	if (::InterlockedCompareExchange(&node->poolState, SESSION_POOL_IN_USE, SESSION_POOL_FREE) != SESSION_POOL_FREE)
	{
		LOGE("free list corrupted : node is not FREE (session %u)", node->session ? node->session->GetSessionID() : UINT32_MAX);
		return nullptr;
	}

	return node->session;
}

void ClientSessionPool::Release(ClientSession* clientSession)
{
	ENGINE_CHECK_RETVOID(clientSession != nullptr, "Release called with a null session");

	const uint32_t sessionId = clientSession->GetSessionID();

	if (!m_nodes || sessionId >= m_capacity)
	{
		LOGE("invalid session id %u (capacity %u)", sessionId, m_capacity);
		return;
	}

	SessionNode* node = &m_nodes[sessionId];

	// 반납은 세션당 정확히 한 번만 수행되어야 한다.
	// 같은 세션에 대해 아래 경로들이 동시에 들어올 수 있다.
	//   - WSASend 실패 -> HandleSocketError -> NotifyDisconnect
	//   - RECV 0바이트(정상 종료) -> HandleSessionDisconnected
	//   - HeartbeatThread 의 좀비 세션 정리
	// 이 전이를 이긴 스레드만 아래 정리를 수행하고 나머지는 즉시 돌아간다.
	// 가드가 없으면 같은 노드가 프리 리스트에 두 번 들어가
	// node->nextNode == node 인 자기참조 루프가 만들어지고,
	// 그 이후 Acquire 는 사용 중인 같은 세션을 반복해서 배포한다.
	if (::InterlockedCompareExchange(&node->poolState, SESSION_POOL_RELEASING, SESSION_POOL_IN_USE) != SESSION_POOL_IN_USE)
	{
		// 동시 disconnect 경로가 실제로 겹쳤다는 신호다. 드물게 발생하고
		// 정상 처리되지만, 프리 리스트 가드가 동작했다는 유일한 증거이므로 남긴다.
		LOGI("session %u release skipped (already releasing or not in use)", sessionId);
		return;
	}

	if (clientSession->IsTransportConnected())
	{
		if (!clientSession->CancelPendingIO())
		{
			ENGINE_VIOLATION("session %u CancelPendingIO failed during release", sessionId);
		}

		if (!clientSession->WaitForIOCancelComplete(10'000))
		{
			// 취소가 완료되지 않았는데도 아래에서 세션을 리셋하고 풀에 반납한다.
			// 남은 완료 통지가 회수된 세션을 만질 수 있다.
			ENGINE_VIOLATION("session %u IO cancel did not complete, releasing it anyway", sessionId);
		}
	}

	if (m_closeSocketFunc)
	{
		m_closeSocketFunc(clientSession->DetachSocket());
	}

	if (!clientSession->OnDisconnect())
	{
		ENGINE_VIOLATION("session %u OnDisconnect reported failure during release", sessionId);
	}

	clientSession->ResetSession();

	{
		Core::Sync::SRWWriteLockGuard lockguard(m_lock);
		node->nextNode = m_freeList;
		m_freeList = node;

		// 프리 리스트에 올린 다음 락 안에서 FREE 로 전이한다.
		// 순서를 바꾸면 Acquire 가 아직 프리 리스트에 없는 노드를 FREE 로 보거나,
		// 반대로 프리 리스트에 있는 노드를 RELEASING 으로 보게 된다.
		::InterlockedExchange(&node->poolState, SESSION_POOL_FREE);
	}
}

uint32_t ClientSessionPool::GetSessionCount() const
{
	return m_capacity;
}

bool ClientSessionPool::IsSessionFull() const
{
	Core::Sync::SRWReadLockGuard lockguard(m_lock);
	return (m_freeList == nullptr);
}

uint32_t ClientSessionPool::GetInUseCount() const
{
	if (!m_nodes)
		return 0;

	uint32_t count = 0;

	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		// poolState 는 Interlocked 로만 전이하므로 원자적으로 읽으면 된다.
		// 세는 도중에 값이 바뀔 수 있어 결과는 근사치다. 접속 수용 판단은
		// 어차피 다음 순간에 또 달라지므로 근사치로 충분하다.
		if (::InterlockedCompareExchange(
			const_cast<volatile LONG*>(&m_nodes[i].poolState), 0, 0) != SESSION_POOL_FREE)
		{
			++count;
		}
	}

	return count;
}

uint32_t ClientSessionPool::CountSessionsFromAddress(const char* ipAddress) const
{
	if (!m_nodes || !m_sessions || !ipAddress || ipAddress[0] == '\0')
		return 0;

	uint32_t count = 0;

	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		if (::InterlockedCompareExchange(
			const_cast<volatile LONG*>(&m_nodes[i].poolState), 0, 0) == SESSION_POOL_FREE)
		{
			continue;
		}

		// 임대 중인 세션의 주소만 본다. 반납된 세션의 주소는 지워진다.
		if (::strncmp(m_sessions[i].GetClientIPAddress(), ipAddress, INET_ADDRSTRLEN) == 0)
		{
			++count;
		}
	}

	return count;
}

ClientSession* ClientSessionPool::GetSession(const uint32_t sessionId)
{
	// 이 함수만 검사가 없었다. 같은 클래스의 Release 도, AcceptSessionPool 의
	// 같은 함수도 범위를 본다. 여기만 빠져 있으면 잘못된 id 하나가 배열 밖을
	// 읽고 그 쓰레기 포인터가 세션으로 유통된다.
	if (!m_nodes)
		return nullptr;

	if (sessionId >= m_capacity)
	{
		LOGE("GetSession called with session id %u but capacity is %u", sessionId, m_capacity);
		return nullptr;
	}

	return m_nodes[sessionId].session;
}

void ClientSessionPool::RequestAllRecvSendIOCancel()
{
	if (!m_sessions)
		return;

	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		ClientSession* session = &m_sessions[i];

		if (session != nullptr && session->IsTransportConnected())
		{
			if (!session->CancelPendingIO())
			{
				ENGINE_VIOLATION("session %u CancelPendingIO failed during bulk cancel", session->GetSessionID());
			}
		}
	}
}

bool ClientSessionPool::WaitForAllRecvSendIOCancelComplete(const uint32_t timeout_ms)
{
	if (!m_sessions)
		return false;

	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		ClientSession* session = &m_sessions[i];

		if (session != nullptr && session->IsTransportConnected())
		{
			if (!session->WaitForIOCancelComplete(timeout_ms))
			{
				ENGINE_VIOLATION("session %u IO cancel did not complete within %u ms", session->GetSessionID(), timeout_ms);
				return false;
			}
		}
	}

	return true;
}

void ClientSessionPool::DisconnectAllSessions()
{
	if (!m_sessions)
		return;

	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		ClientSession* session = &m_sessions[i];

		if (session != nullptr && session->IsTransportConnected())
		{
			if (m_closeSocketFunc)
			{
				m_closeSocketFunc(session->DetachSocket());
			}

			if (!session->OnDisconnect())
			{
				ENGINE_VIOLATION("session %u OnDisconnect reported failure", session->GetSessionID());
			}

			session->ResetSession();
		}
	}
}

uint32_t ClientSessionPool::SendHeartbeatRequests()
{
	if (!m_sessions)
	{
		return 0;
	}

	uint32_t sentCount = 0;

	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		ClientSession* session = &m_sessions[i];
		if (!session || session->GetSessionRole() != SESSION_ROLE::SERVER)
		{
			continue;
		}

		if (!session->IsEstablished() || session->IsSocketInvalid())
		{
			continue;
		}

		if (session->SendSystemHeartbeatRequest())
		{
			++sentCount;
		}
	}

	return sentCount;
}

uint32_t ClientSessionPool::DisconnectZombieSessions(uint64_t nowTick, uint64_t heartbeatTimeout_ms, uint64_t releaseBudget_ms)
{
	if (!m_sessions || heartbeatTimeout_ms == 0)
	{
		return 0;
	}

	const uint64_t passBeginTick = ::GetTickCount64();
	uint32_t disconnectedCount = 0;

	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		ClientSession* session = &m_sessions[i];
		if (!session || !session->IsTransportConnected())
		{
			continue;
		}

		if (session->GetSessionRole() != SESSION_ROLE::SERVER)
		{
			continue;
		}

		if (session->IsSocketInvalid())
		{
			continue;
		}

		if (!session->IsHeartbeatTimedOut(nowTick, heartbeatTimeout_ms))
		{
			continue;
		}

		// 여기서부터가 막힐 수 있는 구간이다.
		
		// Release 안에는 최대 10초짜리 WaitForIOCancelComplete 가 있고,
		// 이 루프는 좀비 수만큼 그 대기를 직렬로 쌓는다. 부르는 쪽은
		// HeartbeatThread 한 스레드이므로 그 시간 동안 하트비트 발송과
		// accept 슬롯 보충이 함께 멈춘다.
		
		// 하트비트 발송이 멈추면 멀쩡한 세션도 응답할 기회를 잃고 좀비로
		// 판정된다. 실측한 연쇄다 — 정리 한 건이 2초일 때 좀비 6개면
		// 발송 간격이 17초로 벌어지고(주기는 5초), 정상 응답하던 세션이
		// 타임아웃 판정으로 끊겼다.
	
		// 그래서 한 주기에 쓸 시간에 상한을 둔다. 남은 좀비는 다음 주기가
		// 가져간다. 한 건은 예산과 무관하게 처리하므로 예산이 아무리
		// 작아도 정리가 영원히 밀리지는 않는다.
		if (disconnectedCount > 0 && ::GetTickCount64() - passBeginTick >= releaseBudget_ms)
		{
			LOGW("zombie release budget of %llu ms is used up after %u sessions. the rest are left for the next cycle",
				releaseBudget_ms, disconnectedCount);
			break;
		}

		LOGW("session %u heartbeat timeout, disconnecting", session->GetSessionID());
		session->MarkHeartbeatTimeout();
		Release(session);
		++disconnectedCount;
	}

	return disconnectedCount;
}

void ClientSessionPool::SetSocketCloseFunc(CloseSocketFunc closeSocketFunc)
{
	m_closeSocketFunc = closeSocketFunc;
}

void ClientSessionPool::SetEventHandler(ISessionEvent* handler)
{
	m_eventHandler = handler;

	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		m_sessions[i].SetEventHandler(m_eventHandler);
	}
}
