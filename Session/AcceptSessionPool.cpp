#include "AcceptSessionPool.h"

#include "AcceptSession.h"

#include <WinSock2.h>

#include "../../CoreLib/Sync/SRWLockGuard.h"

AcceptSessionPool::AcceptSessionPool(uint32_t capacity)
{
	m_capacity = capacity;

	// Accept 세션 풀 생성
	m_sessions = new AcceptSession[m_capacity];
	if (!m_sessions)
		return;

	// Accept 세션 초기화
	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		m_sessions[i].Initialize(SESSION_ROLE::ACCEPT, i);
	}
}

AcceptSessionPool::~AcceptSessionPool()
{
	// RAII 원칙에 의해 소멸자에서 세션풀의 모든 AcceptEx 취소 요청
	RequestAllAcceptIOCancel();

	WaitForAllAcceptIOCancelComplete(10'000);

	// RAII 원칙에 의해 소멸자에서 세션풀 제거
	if (m_sessions)
	{
		delete[] m_sessions;
		m_sessions = nullptr;
	}
}

AcceptSession* AcceptSessionPool::GetSession(const uint32_t sessionId)
{
	if (sessionId >= INVALID_SESSION_ID)
		return nullptr;

	if (sessionId >= m_capacity)
		return nullptr;

	return &m_sessions[sessionId];
}

uint32_t AcceptSessionPool::GetSessionCount() const
{
	return m_capacity;
}

void AcceptSessionPool::RequestAllAcceptIOCancel()
{
	if (!m_sessions)
		return;

	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		AcceptSession* session = &m_sessions[i];

		if (session != nullptr && !session->IsSocketInvalid())
		{
			// Accept Session 은 CancelIOEx 를 호출해도 IO 통지가 오지 않는다.
			// 여기에서 호출해주는 이유는 m_cancelIo 와 m_ioCancelCompleteEvent 를 설정해주기 위함.
			if (!session->CancelPendingIO())
			{
				__debugbreak();
			}

			// Accept Session 은 소켓을 강제로 닫거나, 서버 소켓이 닫히는 경우에만 IO Abort 를 수신한다.
			if (m_closeSocketFunc)
			{
				m_closeSocketFunc(session->DetachSocket());
			}
		}
	}
}

bool AcceptSessionPool::WaitForAllAcceptIOCancelComplete(const uint32_t timeout_ms)
{
	if (!m_sessions)
		return false;

	// [2] Accept 세션의 IO 취소 완료 대기
	// 모든 I/O 가 취소되기를 기다린다.
	// 세션 내부의 IO Count 를 체크해서
	// 0 개가 되면 이벤트가 Set 된다.
	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		AcceptSession* session = &m_sessions[i];

		if (session == nullptr)
		{
			continue;
		}

		if (session->GetAcceptSessionState() != AcceptSessionState::ACCEPT_READY)
		{
			if (!session->WaitForIOCancelComplete(timeout_ms))
			{
				__debugbreak();
				return false;
			}

			session->ResetSession();
		}
	}

	return true;
}

void AcceptSessionPool::SetSocketCloseFunc(CloseSocketFunc closeSocketFunc)
{
	m_closeSocketFunc = closeSocketFunc;
}
