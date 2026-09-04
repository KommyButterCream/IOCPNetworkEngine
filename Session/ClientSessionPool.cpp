#include "ClientSessionPool.h"

#include "ClientSession.h"
#include "SessionNode.h"

#include "../../Core/Sync/SRWLockGuard.h"
#include "../../Core/Util/Logger.h"

using namespace Core::Util;

ClientSessionPool::ClientSessionPool(uint32_t capacity, HybridSendPacketPool* hybridSendPacketPool, SlabMemoryPool* jobMemoryPool, SlabMemoryPool* packetMemoryPool, SlabMemoryPool* generalMemoryPool)
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
			__debugbreak();
			return;
		}

		if (!m_sessions[i].InitializeMemoryPool(hybridSendPacketPool, jobMemoryPool, packetMemoryPool, generalMemoryPool))
		{
			__debugbreak();
			return;
		}

		m_nodes[i].session = &m_sessions[i];
		m_nodes[i].nextNode = m_freeList;
		m_freeList = &m_nodes[i];
	}
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

ISession* ClientSessionPool::Acquire()
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
		Logger::Log(LogLevel::LOG_ERROR, "[%s] free list corrupted : node is not FREE (session %u)", __FUNCTION__, node->session ? node->session->GetSessionID() : UINT32_MAX);
		return nullptr;
	}

	return node->session;
}

void ClientSessionPool::Release(ISession* session)
{
	ClientSession* clientSession = dynamic_cast<ClientSession*>(session);
	if (!clientSession)
	{
		__debugbreak();
		return;
	}

	const uint32_t sessionId = clientSession->GetSessionID();

	if (!m_nodes || sessionId >= m_capacity)
	{
		Logger::Log(LogLevel::LOG_ERROR, "[%s] invalid session id %u (capacity %u)", __FUNCTION__, sessionId, m_capacity);
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
		Logger::Log(LogLevel::LOG_INFO, "[%s][ClientSession : %u] release skipped (already releasing or not in use)", __FUNCTION__, sessionId);
		return;
	}

	if (clientSession->IsConnected())
	{
		if (!clientSession->CancelPendingIO())
		{
			__debugbreak();
		}

		if (!clientSession->WaitForIOCancelComplete(10'000))
		{
			__debugbreak();
		}
	}

	if (m_closeSocketFunc)
	{
		m_closeSocketFunc(clientSession->DetachSocket());
	}

	if (!clientSession->OnDisconnect())
	{
		__debugbreak();
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

ISession* ClientSessionPool::GetSession(const uint32_t sessionId)
{
	return m_nodes[sessionId].session;
}

void ClientSessionPool::RequestAllRecvSendIOCancel()
{
	if (!m_sessions)
		return;

	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		ClientSession* session = &m_sessions[i];

		if (session != nullptr && session->IsConnected())
		{
			if (!session->CancelPendingIO())
			{
				__debugbreak();
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

		if (session != nullptr && session->IsConnected())
		{
			if (!session->WaitForIOCancelComplete(timeout_ms))
			{
				__debugbreak();
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

		if (session != nullptr && session->IsConnected())
		{
			if (m_closeSocketFunc)
			{
				m_closeSocketFunc(session->DetachSocket());
			}

			if (!session->OnDisconnect())
			{
				__debugbreak();
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

uint32_t ClientSessionPool::DisconnectZombieSessions(uint64_t nowTick, uint64_t heartbeatTimeout_ms)
{
	if (!m_sessions || heartbeatTimeout_ms == 0)
	{
		return 0;
	}

	uint32_t disconnectedCount = 0;

	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		ClientSession* session = &m_sessions[i];
		if (!session || !session->IsConnected())
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

		Logger::Log(LogLevel::LOG_WARNING, "[%s][ClientSession : %d] heartbeat timeout detected", __FUNCTION__, session->GetSessionID());
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
