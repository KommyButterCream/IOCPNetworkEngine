#pragma once

#include <stdint.h>

#include "ISession.h"
#include "SessionDefs.h"

#ifdef BUILD_IOCP_ENGINE_DLL
#define IOCP_ENGINE_API __declspec(dllexport)
#else
#define IOCP_ENGINE_API __declspec(dllimport)
#endif

// 세션의 공통 상태와 I/O 수명 관리.
class IOCP_ENGINE_API BaseSession : public ISession
{
public:
	BaseSession();
	~BaseSession() override;

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
	// --- 확장점. 파생 클래스가 재정의한다 ---
	virtual bool Initialize(SESSION_ROLE sessionType, uint32_t sessionId);
	virtual void ResetSession();
	virtual void Finalize();

	virtual bool OnAccept() = 0;
	virtual bool OnConnect() = 0;
	virtual bool OnDisconnect();

	// --- 소켓 제어 ---
	//
	// AttachSocket 과 DetachSocket 은 짝이다. 붙일 때는 이미 들고 있지
	// 않은지 검사하므로(사용 중인 세션이 재배포되면 그 경로로 온다)
	// 본문이 .cpp 에 있다.
	void AttachSocket(SOCKET socket);
	SOCKET DetachSocket();

	void SetClientSocket(SOCKET socket) { m_clientSocket = socket; }
	SOCKET GetClientSocket() const { return m_clientSocket; }
	bool IsSocketInvalid() const { return (m_clientSocket == INVALID_SOCKET); }

	// --- 상태 관리 ---
	//
	// 역할별로 쓰는 것이 하나씩 정해져 있다. accept 세션은 Accept 상태만,
	// 서버 역할 세션은 Server 상태만 쓴다.
	void SetClientSessionState(ClientSessionState sessionState) { m_clientSessionState = sessionState; }
	ClientSessionState GetClientSessionState() const { return m_clientSessionState; }

	void SetServerSessionState(ServerSessionState sessionState) { m_serverSessionState = sessionState; }
	ServerSessionState GetServerSessionState() const { return m_serverSessionState; }

	void SetAcceptSessionState(AcceptSessionState sessionState) { m_acceptSessionState = sessionState; }
	AcceptSessionState GetAcceptSessionState() const { return m_acceptSessionState; }

	// --- 타입 및 식별자 ---
	SESSION_ROLE GetSessionRole() const { return m_sessionRole; }
	void SetSessionID(uint32_t sessionId) { m_sessionId = sessionId; }

	// ISession 의 유일한 순수 가상. 서비스가 ISession* 로 부른다.
	uint32_t GetSessionID() const override { return m_sessionId; }

	// --- I/O 수명 ---
	//
	// 엔진 내부 전용. ISession 에는 없다 (이유는 그쪽 주석).
	void IncrementIO();
	void DecrementIO();

	bool CancelPendingIO();
	bool WaitForIOCancelComplete(const uint32_t timeout_ms);
};
