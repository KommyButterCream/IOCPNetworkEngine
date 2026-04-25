#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>
#include <stdint.h>

#include "ISessionEvent.h"

class ISession;
class ClientSessionPool;
class AcceptSessionPool;
class HybridSendPacketPool;
class SlabMemoryPool;

class SessionManager : public ISessionEvent
{
public:
	SessionManager();
	~SessionManager();

private:
	SessionManager(const SessionManager& rhs) = delete;
	SessionManager& operator = (const SessionManager& rhs) = delete;
	SessionManager(SessionManager&& rhs) noexcept = delete;
	SessionManager& operator = (SessionManager&& rhs) noexcept = delete;

private:
	AcceptSessionPool* m_acceptSessionPool = nullptr;
	ClientSessionPool* m_clientSessionPool = nullptr;

	uint32_t m_acceptSessionCount = 0;
	uint32_t m_clientSessionCount = 0;

public:
	bool Initialize(const uint32_t acceptSessionCount, const uint32_t clientSessionCount, HybridSendPacketPool* hybridSendPacketPool, SlabMemoryPool* jobMemoryPool, SlabMemoryPool* packetMemoryPool, SlabMemoryPool* generalMemoryPool, CloseSocketFunc closeSocketFunc);
	void Finalize();

	// Accept Session //
public:
	ISession* GetAcceptSession(const uint32_t sessionId);
	uint32_t GetAcceptSessionCount() const noexcept;

	void RequestAllAcceptIOCancel();
	bool WaitForAllAcceptIOCancelComplete(const uint32_t timeout_ms);

	// Client Session //
public:
	bool IsClientSessionFull() const;

	ISession* GetClientSession(const uint32_t sessionId);
	uint32_t GetClientSessionCount() const noexcept;

	ISession* AcquireClientSession();
	void ReleaseClientSession(ISession* session);

	void RequestAllRecvSendIOCancel();
	bool WaitForAllRecvSendIOCancelComplete(const uint32_t timeout_ms);
	void DisconnectAllSessions();
	uint32_t SendHeartbeatRequests();
	uint32_t DisconnectZombieSessions(uint64_t heartbeatTimeout_ms);

	void OnDisconnectRequest(ISession* session) override;
};
