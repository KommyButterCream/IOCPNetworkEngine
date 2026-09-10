#include "AcceptSession.h"

#include <WinSock2.h>

#include "../Diagnostics/EngineAssert.h"

#include "../../Core/Util/Logger.h"

using namespace Core::Util;

#pragma comment(lib, "ws2_32.lib")

AcceptSession::AcceptSession()
{
	m_acceptOverlapped.operation = IO_OPERATION::ACCEPT;
}

AcceptSession::~AcceptSession()
{
	Finalize();
}

bool AcceptSession::Initialize(SESSION_ROLE sessionType, uint32_t sessionId)
{
	if (!BaseSession::Initialize(sessionType, sessionId))
		return false;

	SetAcceptSessionState(AcceptSessionState::ACCEPT_READY);
	return true;
}

void AcceptSession::ResetSession()
{
	BaseSession::ResetSession();
	SetAcceptSessionState(AcceptSessionState::ACCEPT_READY);

	::ZeroMemory(&m_acceptBuffer, sizeof(m_acceptBuffer));

	m_acceptOverlapped.Clear();
}

void AcceptSession::Finalize()
{
	if (m_destroyFlag)
	{
		return;
	}

	BaseSession::Finalize();

	::ZeroMemory(&m_acceptBuffer, sizeof(m_acceptBuffer));

	m_acceptOverlapped.Clear();

	m_destroyFlag = true;
}

bool AcceptSession::OnAccept()
{
	SetAcceptSessionState(AcceptSessionState::ACCEPT_COMPLETE);

	// 접속 1건당 호출된다. HandleAccept 가 같은 사실을 더 자세히 남기므로
	// 여기서는 추적 레벨로만 둔다.
	LOGT("accept session %u complete", GetSessionID());

	return true;
}

bool AcceptSession::OnConnect()
{
	// accept 세션은 접속을 거는 쪽이 아니다. OnConnect 를 부르는 곳은
	// ConnectEx 완료 처리(IOCPClient)와 accept 완료 처리(IOCPServer)뿐이고
	// 둘 다 ClientSession 을 상대한다. 여기 왔다면 라우팅이 깨진 것이다.
	//
	// 예전에는 조용히 false 만 돌려줬다. 호출부는 그걸 "접속 훅이 실패했다"
	// 로 읽고 연결을 끊는데, 진짜 원인인 잘못된 라우팅은 아무 데도 남지 않는다.
	ENGINE_VIOLATION("accept session %u OnConnect was called. accept sessions never connect",
		GetSessionID());

	return false;
}

bool AcceptSession::OnDisconnect()
{
	if (::InterlockedExchange(&m_closing, 1) == 0)
	{
		LOGT("accept session %u disconnecting", GetSessionID());

		if (!IsSocketInvalid())
		{
			// 소켓은 반드시 DetachSocket 으로 먼저 떼어낸 뒤 여기 와야 한다.
			// 그러지 않으면 소켓이 닫히지 않고 새는 상태로 세션이 회수된다.
			LOGE("accept session %u disconnecting with a live socket %d. it was not detached",
				GetSessionID(), static_cast<int>(GetClientSocket()));
			ENGINE_BREAK_IF_DEBUGGER();
			return false;
		}

		if (GetAcceptSessionState() != AcceptSessionState::ACCEPT_ABORTED)
		{
			SetAcceptSessionState(AcceptSessionState::DISCONNECTED);
		}
	}

	return true;
}

OverlappedEx& AcceptSession::GetAcceptOverlapped()
{
	return m_acceptOverlapped;
}

char* AcceptSession::GetAcceptBuffer()
{
	return m_acceptBuffer;
}
