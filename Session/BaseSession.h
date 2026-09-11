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

	// 남은 I/O 가 0 이 되는 순간 부를 마무리 콜백.
	//
	// 세션은 자기를 담은 풀을 모른다(알면 풀 종류마다 세션이 갈라진다).
	// 그래서 풀이 자기 마무리 절차를 함수 포인터로 맡긴다.
	// ClientSessionPool 이 m_closeSocketFunc 를 넘기는 방식과 같다.
	using ReleaseReadyFunc = void (*)(void* context, BaseSession* session);

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

	// 반납 예약. 1 이면 "남은 I/O 가 다 끝나는 대로 이 세션을 반납하라".
	//
	// 이 플래그는 예약이면서 동시에 소유권 토큰이다. 1 -> 0 전이에
	// 성공한 스레드 하나만 마무리를 수행한다. RequestRelease 를 부른
	// 스레드와 마지막 DecrementIO 를 하는 스레드가 동시에 "내가 마지막"
	// 이라고 판단할 수 있어, 둘 중 하나만 고르는 장치가 필요하다.
	volatile LONG m_releasePending = 0;

	ReleaseReadyFunc m_releaseReadyFunc = nullptr;
	void* m_releaseReadyContext = nullptr;

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

	// --- 지연 반납 ---
	//
	// 완료 핸들러 안에서 반납이 필요해지는 경로가 여럿이다. 그때 그 자리에서
	// 카운트가 0 이 되기를 기다리면, 핸들러 자신이 들고 있는 몫 때문에
	// 자기를 기다리게 된다(실측: bench 실행당 200회 이상의 10초 타임아웃).
	//
	// 그래서 기다리지 않는다. 반납을 예약해 두고, 마지막 완료가 카운트를
	// 0 으로 내리는 그 자리에서 마무리한다.
	void SetReleaseReadyFunc(ReleaseReadyFunc releaseReadyFunc, void* context);

	// 반납을 예약한다.
	//   true  : 남은 I/O 가 없고 마무리 권한을 이 스레드가 잡았다.
	//           부르는 쪽이 그 자리에서 마무리해야 한다.
	//   false : 아직 완료되지 않은 I/O 가 있거나, 다른 스레드가 이미
	//           권한을 잡았다. 부르는 쪽은 즉시 돌아간다.
	bool RequestRelease();

	// 반납이 예약되어 있는가. 핸들러가 더 진행할지 판단할 때 쓴다.
	bool IsReleasePending() const;

	// --- 진단 ---
	//
	// 지금 이 세션이 소유권을 주장 중인 I/O 수. 0 이면 어떤 스레드도
	// 완료 핸들러 안에서 이 세션을 만지고 있지 않다는 뜻이고, 반납이
	// 안전하다는 판정의 근거가 이 값 하나다.
	// 하네스가 시나리오마다 회계를 검사하려면 밖에서 읽을 수 있어야 한다.
	LONG GetOutstandingIOCount() const;
};
