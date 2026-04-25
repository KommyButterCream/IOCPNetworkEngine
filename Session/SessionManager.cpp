#include "SessionManager.h"

#include "AcceptSession.h"
#include "AcceptSessionPool.h"
#include "ClientSession.h"
#include "ClientSessionPool.h"

SessionManager::SessionManager()
{
}

SessionManager::~SessionManager()
{
	Finalize();
}

bool SessionManager::Initialize(const uint32_t acceptSessionCount, const uint32_t clientSessionCount, HybridSendPacketPool* hybridSendPacketPool, SlabMemoryPool* jobMemoryPool, SlabMemoryPool* packetMemoryPool, SlabMemoryPool* generalMemoryPool, CloseSocketFunc closeSocketFunc)
{
	m_acceptSessionCount = acceptSessionCount;
	m_clientSessionCount = clientSessionCount;

	m_acceptSessionPool = new AcceptSessionPool(acceptSessionCount);
	if (!m_acceptSessionPool)
		return false;

	m_acceptSessionPool->SetSocketCloseFunc(closeSocketFunc);

	m_clientSessionPool = new ClientSessionPool(clientSessionCount, hybridSendPacketPool, jobMemoryPool, packetMemoryPool, generalMemoryPool);
	if (!m_clientSessionPool)
		return false;

	m_clientSessionPool->SetSocketCloseFunc(closeSocketFunc);
	m_clientSessionPool->SetEventHandler(this);

	return true;
}

void SessionManager::Finalize()
{
	m_acceptSessionCount = 0;
	m_clientSessionCount = 0;

	if (m_acceptSessionPool)
	{
		delete m_acceptSessionPool;
		m_acceptSessionPool = nullptr;
	}

	if (m_clientSessionPool)
	{
		delete m_clientSessionPool;
		m_clientSessionPool = nullptr;
	}
}

ISession* SessionManager::GetAcceptSession(const uint32_t sessionId)
{
	if (!m_acceptSessionPool)
		return nullptr;

	if (sessionId >= m_acceptSessionPool->GetSessionCount())
		return nullptr;

	return m_acceptSessionPool->GetSession(sessionId);
}

uint32_t SessionManager::GetAcceptSessionCount() const noexcept
{
	return m_acceptSessionCount;
}

void SessionManager::RequestAllAcceptIOCancel()
{
	if (!m_acceptSessionPool)
		return;

	m_acceptSessionPool->RequestAllAcceptIOCancel();
}

bool SessionManager::WaitForAllAcceptIOCancelComplete(const uint32_t timeout_ms)
{
	if (!m_acceptSessionPool)
		return false;

	return m_acceptSessionPool->WaitForAllAcceptIOCancelComplete(timeout_ms);
}

bool SessionManager::IsClientSessionFull() const
{
	return m_clientSessionPool->IsSessionFull();
}

ISession* SessionManager::GetClientSession(const uint32_t sessionId)
{
	if (!m_clientSessionPool)
		return nullptr;

	if (sessionId >= m_clientSessionPool->GetSessionCount())
		return nullptr;

	return m_clientSessionPool->GetSession(sessionId);
}

uint32_t SessionManager::GetClientSessionCount() const noexcept
{
	return m_clientSessionCount;
}

ISession* SessionManager::AcquireClientSession()
{
	if (!m_clientSessionPool)
		return nullptr;

	ISession* session = nullptr;

	constexpr int maxRetryCount = 100;
	int retryCount = 0;

	do
	{
		session = m_clientSessionPool->Acquire();
		++retryCount;
	} while (!session && retryCount < maxRetryCount);

	if (retryCount == maxRetryCount)
	{
		__debugbreak();
	}

	return session;
}

void SessionManager::ReleaseClientSession(ISession* session)
{
	if (!m_clientSessionPool)
		return;

	if (session == nullptr)
	{
		__debugbreak();
		return;
	}

	const uint32_t sessionId = session->GetSessionID();
	if (sessionId >= m_clientSessionPool->GetSessionCount())
	{
		__debugbreak();
		return;
	}

	m_clientSessionPool->Release(session);
}

void SessionManager::RequestAllRecvSendIOCancel()
{
	if (!m_clientSessionPool)
		return;

	m_clientSessionPool->RequestAllRecvSendIOCancel();
}

bool SessionManager::WaitForAllRecvSendIOCancelComplete(const uint32_t timeout_ms)
{
	if (!m_clientSessionPool)
		return false;

	return m_clientSessionPool->WaitForAllRecvSendIOCancelComplete(timeout_ms);
}

void SessionManager::DisconnectAllSessions()
{
	if (!m_clientSessionPool)
		return;

	m_clientSessionPool->DisconnectAllSessions();
}

uint32_t SessionManager::SendHeartbeatRequests()
{
	if (!m_clientSessionPool)
	{
		return 0;
	}

	return m_clientSessionPool->SendHeartbeatRequests();
}

uint32_t SessionManager::DisconnectZombieSessions(uint64_t heartbeatTimeout_ms)
{
	if (!m_clientSessionPool || heartbeatTimeout_ms == 0)
	{
		return 0;
	}

	return m_clientSessionPool->DisconnectZombieSessions(::GetTickCount64(), heartbeatTimeout_ms);
}

void SessionManager::OnDisconnectRequest(ISession* session)
{
	if (!m_clientSessionPool)
		return;

	ReleaseClientSession(session);
}
