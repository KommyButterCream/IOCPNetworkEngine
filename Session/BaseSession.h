#pragma once

#include <stdint.h>

#include "ISession.h"
#include "SessionDefs.h"

#ifdef BUILD_IOCP_ENGINE_DLL
#define IOCP_ENGINE_API __declspec(dllexport)
#else
#define IOCP_ENGINE_API __declspec(dllimport)
#endif

class IOCP_ENGINE_API BaseSession : public ISession
{
public:
	BaseSession();
	virtual ~BaseSession();

protected:
	bool m_destroyFlag = false;

	SESSION_ROLE m_sessionRole = SESSION_ROLE::NONE;
	ClientSessionState m_clientSessionState = ClientSessionState::NONE;
	ServerSessionState m_serverSessionState = ServerSessionState::NONE;
	AcceptSessionState m_acceptSessionState = AcceptSessionState::NONE;

	SOCKET m_clientSocket = INVALID_SOCKET;

	uint32_t m_sessionId = INVALID_SESSION_ID;

	volatile LONG m_closing = 0;
	volatile LONG m_ioCount = 0;
	volatile LONG m_cancelIo = 0;

	HANDLE m_ioCancelCompleteEvent = nullptr;

public:
	virtual bool Initialize(SESSION_ROLE sessionType, uint32_t sessionId);
	virtual void ResetSession();
	virtual void Finalize();

	virtual void SetClientSocket(SOCKET socket);
	virtual SOCKET GetClientSocket() const;

	virtual void AttachSocket(SOCKET socket);
	virtual SOCKET DetachSocket();

	bool IsSocketInvalid() const;

	virtual void SetClientSessionState(ClientSessionState sessionState);
	virtual ClientSessionState GetClientSessionState() const;

	virtual void SetServerSessionState(ServerSessionState sessionState);
	virtual ServerSessionState GetServerSessionState() const;

	virtual void SetAcceptSessionState(AcceptSessionState sessionState);
	virtual AcceptSessionState GetAcceptSessionState() const;

	virtual SESSION_ROLE GetSessionRole() const;
	virtual uint32_t GetSessionID() const;
	virtual void SetSessionID(uint32_t sessionId);

	virtual void IncrementIO();
	virtual void DecrementIO();

	virtual bool CancelPendingIO();
	virtual bool WaitForIOCancelComplete(const uint32_t timeout_ms);

	virtual bool OnAccept() = 0;
	virtual bool OnConnect() = 0;
	virtual bool OnDisconnect();
};
