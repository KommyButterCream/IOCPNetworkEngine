#pragma once

#include <WinSock2.h>
#include <stdint.h>

#include "SessionDefs.h"

#ifdef BUILD_IOCP_ENGINE_DLL
#define IOCP_ENGINE_API __declspec(dllexport)
#else
#define IOCP_ENGINE_API __declspec(dllimport)
#endif

enum class ClientSessionState
{
	NONE,
	CONNECT_READY,
	CONNECTING,
	CONNECTED,
	AUTH_PENDING,
	ESTABLISHED,
	CONNECT_ABORTED,
	DISCONNECTED,
};

enum class ServerSessionState
{
	NONE,
	CONNECT_READY,
	CONNECTED,
	AUTH_PENDING,
	ESTABLISHED,
	HEARTBEAT_TIMEOUT,
	DISCONNECTED,
};

enum class AcceptSessionState
{
	NONE,
	ACCEPT_READY,
	ACCEPT_WAIT,
	ACCEPT_COMPLETE,
	ACCEPT_ABORTED,
	DISCONNECTED,
};

class IOCP_ENGINE_API ISession
{
public:
	virtual ~ISession() = default;

	// --- 기본 동작 ---
	virtual bool Initialize(SESSION_ROLE sessionType, uint32_t sessionId) = 0;
	virtual void ResetSession() = 0;
	virtual void Finalize() = 0;

	// --- 소켓 제어 ---
	virtual void SetClientSocket(SOCKET socket) = 0;
	virtual SOCKET GetClientSocket() const = 0;

	// --- 상태 관리 ---
	virtual void SetClientSessionState(ClientSessionState sessionState) = 0;
	virtual ClientSessionState GetClientSessionState() const = 0;

	virtual void SetServerSessionState(ServerSessionState sessionState) = 0;
	virtual ServerSessionState GetServerSessionState() const = 0;

	virtual void SetAcceptSessionState(AcceptSessionState sessionState) = 0;
	virtual AcceptSessionState GetAcceptSessionState() const = 0;


	// --- 타입 및 식별자 ---
	virtual SESSION_ROLE GetSessionRole() const = 0;
	virtual uint32_t GetSessionID() const = 0;
	virtual void SetSessionID(uint32_t sessionId) = 0;

	// --- IO 카운팅 (Interlocked) ---
	virtual void IncrementIO() = 0;
	virtual void DecrementIO() = 0;
};
