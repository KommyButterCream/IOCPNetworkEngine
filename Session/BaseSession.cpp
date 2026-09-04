#include "BaseSession.h"

#include "SessionDefs.h"

#include "../Diagnostics/EngineAssert.h"

#include "../../Core/Util/Logger.h"

using namespace Core::Util;

BaseSession::BaseSession()
{
}

BaseSession::~BaseSession()
{
	Finalize();
}

bool BaseSession::Initialize(SESSION_ROLE sessionType, uint32_t sessionId)
{
	m_sessionRole = sessionType;
	SetSessionID(sessionId);

	if (!m_ioCancelCompleteEvent)
	{
		m_ioCancelCompleteEvent = ::CreateEvent(nullptr, TRUE, FALSE, nullptr);
		if (!m_ioCancelCompleteEvent)
			return false;
	}
	else
	{
		::ResetEvent(m_ioCancelCompleteEvent);
	}

	return true;
}

void BaseSession::ResetSession()
{
	m_clientSessionState = ClientSessionState::NONE;
	m_serverSessionState = ServerSessionState::NONE;
	m_acceptSessionState = AcceptSessionState::NONE;

	if (m_clientSocket != INVALID_SOCKET)
	{
		ENGINE_VIOLATION("session %u reset with a live socket %d. it was not detached",
			GetSessionID(), static_cast<int>(m_clientSocket));
	}

	// IO 카운트는 여기서 강제로 0 으로 만들지 않는다.
	//
	// 이전에는 InterlockedExchange(&m_ioCount, 0) 였다. 그런데 이 시점에
	// 다른 스레드의 완료 처리가 아직 돌고 있으면, 그 스레드의 DecrementIO 가
	// 0 에서 하나 더 내려가 -1 이 되고 단정에 걸렸다. 실제로 관측된 현상이다.
	//
	// 카운트를 그대로 두면 늦게 도착한 DecrementIO 가 정상적으로 0 으로 내려간다.
	// 0 이 아닌 상태로 여기 도달한 것 자체가 버그이므로 크게 남긴다.
	const LONG remainingIo = ::InterlockedCompareExchange(&m_ioCount, 0, 0);
	if (remainingIo != 0)
	{
		ENGINE_VIOLATION("session %u reset while %ld IO operations are still outstanding. the counter is left as is so a late DecrementIO does not go negative",
			GetSessionID(), remainingIo);
	}

	::InterlockedExchange(&m_closing, 0);
	::InterlockedExchange(&m_cancelIo, 0);

	if (m_ioCancelCompleteEvent)
	{
		::ResetEvent(m_ioCancelCompleteEvent);
	}
}

void BaseSession::Finalize()
{
	if (m_destroyFlag)
	{
		return;
	}

	// 종료 시점에는 IOCP 가 이미 멈춰 있을 수 있고, 그때 큐에 남아 있던 완료
	// 통지는 버려진다. 그러면 그 몫의 DecrementIO 가 영영 실행되지 않아
	// 카운트가 0 이 아닌 상태로 여기 도달한다.
	// 종료 경로이므로 치명적이지 않다. 남기고 계속 진행한다.
	const LONG remainingIo = ::InterlockedCompareExchange(&m_ioCount, 0, 0);
	if (remainingIo != 0)
	{
		ENGINE_VIOLATION("session %u finalized while %ld IO operations are still outstanding. completions were most likely dropped when the IOCP stopped",
			GetSessionID(), remainingIo);
	}

	m_clientSessionState = ClientSessionState::NONE;
	m_serverSessionState = ServerSessionState::NONE;
	m_acceptSessionState = AcceptSessionState::NONE;

	if (m_clientSocket != INVALID_SOCKET)
	{
		ENGINE_VIOLATION("session %u finalized with a live socket %d. it was not detached",
			GetSessionID(), static_cast<int>(m_clientSocket));
	}

	::InterlockedExchange(&m_closing, 0);
	::InterlockedExchange(&m_ioCount, 0);
	::InterlockedExchange(&m_cancelIo, 0);

	if (m_ioCancelCompleteEvent)
	{
		::ResetEvent(m_ioCancelCompleteEvent);
		::CloseHandle(m_ioCancelCompleteEvent);
		m_ioCancelCompleteEvent = nullptr;
	}

	m_destroyFlag = true;
}

void BaseSession::SetClientSocket(SOCKET socket)
{
	m_clientSocket = socket;
}

SOCKET BaseSession::GetClientSocket() const
{
	return m_clientSocket;
}

void BaseSession::SetClientSessionState(ClientSessionState sessionState)
{
	m_clientSessionState = sessionState;
}

ClientSessionState BaseSession::GetClientSessionState() const
{
	return m_clientSessionState;
}

void BaseSession::SetServerSessionState(ServerSessionState sessionState)
{
	m_serverSessionState = sessionState;
}

ServerSessionState BaseSession::GetServerSessionState() const
{
	return m_serverSessionState;
}

void BaseSession::SetAcceptSessionState(AcceptSessionState sessionState)
{
	m_acceptSessionState = sessionState;
}

AcceptSessionState BaseSession::GetAcceptSessionState() const
{
	return m_acceptSessionState;
}

SESSION_ROLE BaseSession::GetSessionRole() const
{
	return m_sessionRole;
}

uint32_t BaseSession::GetSessionID() const
{
	return m_sessionId;
}

void BaseSession::SetSessionID(uint32_t sessionId)
{
	m_sessionId = sessionId;
}

void BaseSession::IncrementIO()
{
	::InterlockedIncrement(&m_ioCount);
}

void BaseSession::DecrementIO()
{
	LONG ioCount = ::InterlockedDecrement(&m_ioCount);

	if (ioCount < 0)
	{
		// IncrementIO 없이 DecrementIO 가 불렸거나, 카운터가 외부에서 리셋됐다.
		// ResetSession 이 더 이상 카운터를 강제로 0 으로 만들지 않으므로
		// 이제 이 로그가 뜨면 Increment/Decrement 짝이 실제로 맞지 않는다는 뜻이다.
		ENGINE_VIOLATION("session %u IO count went negative (%ld). an Increment/Decrement pair is unbalanced",
			GetSessionID(), ioCount);
	}

	if (::InterlockedCompareExchange(&m_cancelIo, 0, 0) == 1 && ioCount == 0 && m_ioCancelCompleteEvent)
	{
		LOGI("session %u all pending IO cancelled", GetSessionID());
		::SetEvent(m_ioCancelCompleteEvent);
	}
}

bool BaseSession::CancelPendingIO()
{
	if (m_clientSocket != INVALID_SOCKET)
	{
		if (::InterlockedExchange(&m_cancelIo, 1) == 0)
		{
			LOGI("session %u cancelling pending IO (count %ld)",
				GetSessionID(), ::InterlockedCompareExchange(&m_ioCount, 0, 0));

			if (!::CancelIoEx(reinterpret_cast<HANDLE>(m_clientSocket), nullptr))
			{
				const DWORD errorCode = ::GetLastError();

				if (errorCode == ERROR_NOT_FOUND || errorCode == ERROR_INVALID_HANDLE)
				{
					// CancelIoEx 가 취소할 대상을 찾지 못했다.
					//
					// 이전 코드는 여기서 IO 카운트를 보지 않고 무조건 완료 이벤트를
					// 세웠다. 그러면 실제로는 완료 통지를 기다리는 I/O 가 남아 있는데도
					// WaitForIOCancelComplete 가 즉시 통과해서, 호출부가 아직 사용 중인
					// 세션을 리셋하고 풀로 반납했다. 대기 자체가 무의미했다.
					//
					// 카운트가 0 일 때만 세운다. 0 이 아니면 남은 완료 통지가 도착해
					// DecrementIO 가 0 으로 내릴 때 그쪽에서 이벤트를 세운다.
					const LONG outstanding = ::InterlockedCompareExchange(&m_ioCount, 0, 0);

					if (outstanding == 0)
					{
						if (m_ioCancelCompleteEvent)
						{
							::SetEvent(m_ioCancelCompleteEvent);
						}

						LOGI("session %u CancelIoEx found no pending IO and the count is zero (error %lu)",
							GetSessionID(), errorCode);
					}
					else
					{
						LOGW("session %u CancelIoEx found nothing to cancel but %ld IO operations are still counted. waiting for their completions",
							GetSessionID(), outstanding);
					}

					return true;
				}
				else if (errorCode == ERROR_OPERATION_ABORTED)
				{
					LOGW("session %u CancelIoEx returned ERROR_OPERATION_ABORTED", GetSessionID());
					return false;
				}
				else
				{
					LOGE("session %u CancelIoEx failed (error %lu)", GetSessionID(), errorCode);
					return false;
				}
			}
		}
	}

	return true;
}

bool BaseSession::WaitForIOCancelComplete(const uint32_t timeout_ms)
{
	if (m_ioCancelCompleteEvent)
	{
		DWORD waitResult = ::WaitForSingleObject(m_ioCancelCompleteEvent, timeout_ms);

		switch (waitResult)
		{
		case WAIT_OBJECT_0:
			LOGI("session %u IO cancel completed", GetSessionID());
			return true;

		case WAIT_TIMEOUT:
			// 호출부는 이 실패를 무시하고 세션 해제로 진행하므로
			// 남아 있는 IO 수까지 남겨야 원인 추적이 가능하다.
			LOGE("session %u IO cancel timed out after %u ms (io count still %ld)",
				GetSessionID(), timeout_ms, ::InterlockedCompareExchange(&m_ioCount, 0, 0));
			return false;

		default:
			LOGE("session %u IO cancel wait returned %lu (error %lu)",
				GetSessionID(), waitResult, ::GetLastError());
			return false;
		}
	}

	return true;
}

bool BaseSession::OnDisconnect()
{
	if (::InterlockedExchange(&m_closing, 1) == 0)
	{
		if (m_clientSocket != INVALID_SOCKET)
		{
			// 소켓은 반드시 DetachSocket 으로 먼저 떼어낸 뒤 여기 와야 한다.
			LOGE("session %u disconnecting with a live socket %d. it was not detached",
				GetSessionID(), static_cast<int>(m_clientSocket));
			ENGINE_BREAK_IF_DEBUGGER();
		}

		switch (m_sessionRole)
		{
		case SESSION_ROLE::CLIENT:
			m_clientSessionState = ClientSessionState::DISCONNECTED;
			break;
		case SESSION_ROLE::SERVER:
			m_serverSessionState = ServerSessionState::DISCONNECTED;
			break;
		case SESSION_ROLE::ACCEPT:
			m_acceptSessionState = AcceptSessionState::DISCONNECTED;
			break;
		default:
			break;
		}

		LOGI("session %u disconnected (role %d)", GetSessionID(), static_cast<int>(m_sessionRole));
	}

	return true;
}

void BaseSession::AttachSocket(SOCKET socket)
{
	if (m_clientSocket != INVALID_SOCKET)
	{
		// 이미 소켓을 들고 있는 세션에 다시 붙이려 한다.
		// 사용 중인 세션이 풀에서 다시 배포되면 이 경로로 온다.
		ENGINE_VIOLATION("session %u already holds socket %d, cannot attach socket %d. the session was handed out while still in use",
			GetSessionID(), static_cast<int>(m_clientSocket), static_cast<int>(socket));
	}

	m_clientSocket = socket;
}

SOCKET BaseSession::DetachSocket()
{
	SOCKET socket = m_clientSocket;
	m_clientSocket = INVALID_SOCKET;
	return socket;
}

bool BaseSession::IsSocketInvalid() const
{
	return (m_clientSocket == INVALID_SOCKET);
}
