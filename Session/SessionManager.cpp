#include "SessionManager.h"

#include "AcceptSession.h"

#include "../Diagnostics/EngineAssert.h"

using namespace Core::Util;
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

bool SessionManager::Initialize(const uint32_t acceptSessionCount, const uint32_t clientSessionCount, HybridSendPacketPool* hybridSendPacketPool, EngineMemoryPool* jobMemoryPool, EngineMemoryPool* packetMemoryPool, EngineMemoryPool* generalMemoryPool, CloseSocketFunc closeSocketFunc, const SessionBufferConfig& bufferConfig)
{
	m_acceptSessionCount = acceptSessionCount;
	m_clientSessionCount = clientSessionCount;

	m_acceptSessionPool = new AcceptSessionPool(acceptSessionCount);
	if (!m_acceptSessionPool)
		return false;

	m_acceptSessionPool->SetSocketCloseFunc(closeSocketFunc);

	m_clientSessionPool = new ClientSessionPool(clientSessionCount, hybridSendPacketPool, jobMemoryPool, packetMemoryPool, generalMemoryPool, bufferConfig);
	if (!m_clientSessionPool || !m_clientSessionPool->IsReady())
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

	// 이전에는 실패 시 100회 busy retry 를 돌렸다. Acquire 가 nullptr 이면
	// 프리 리스트가 비었다는 뜻이고, 양보 없는 루프로는 채워질 수 없으므로
	// CPU 만 태우는 코드였다. 게다가 정확히 100번째에 성공하면
	// retryCount == maxRetryCount 가 참이 되어 성공했는데도 단정에 걸렸다.
	ISession* session = m_clientSessionPool->Acquire();

	if (!session)
	{
		// 호출부(HandleAccept)가 연결을 거절하는 정상 경로다.
		LOGW("client session pool is exhausted (capacity %u), the connection will be rejected",
			m_clientSessionPool->GetSessionCount());
	}

	return session;
}

void SessionManager::ReleaseClientSession(ISession* session)
{
	if (!m_clientSessionPool)
		return;

	ENGINE_CHECK_RETVOID(session != nullptr, "ReleaseClientSession called with a null session");

	const uint32_t sessionId = session->GetSessionID();
	ENGINE_CHECK_RETVOID(sessionId < m_clientSessionPool->GetSessionCount(),
		"ReleaseClientSession called with session id %u but capacity is %u",
		sessionId, m_clientSessionPool->GetSessionCount());

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
