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

	// IO 카운팅(IncrementIO / DecrementIO)은 여기에 두지 않는다.
	//
	// 이 인터페이스는 서비스가 패킷 핸들러에서 받는 타입이다. 발행/완료
	// 짝으로만 움직여야 하는 엔진 내부 카운터를 그 자리에 노출하면,
	// 핸들러가 한 번 잘못 부르는 것만으로 세션이 영구히 취소 대기에
	// 묶이거나 사용 중인 세션이 풀로 반납된다.
	//
	// 실제 선언은 BaseSession 에 있고, 엔진은 구체 타입으로만 다룬다.
};
