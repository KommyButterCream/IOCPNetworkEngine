#include "BaseSession.h"

#include "SessionDefs.h"

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
		__debugbreak();
	}

	::InterlockedExchange(&m_closing, 0);
	::InterlockedExchange(&m_ioCount, 0);
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

	LONG remainingIo = ::InterlockedCompareExchange(&m_ioCount, 0, 0);
	if (remainingIo != 0)
	{
		__debugbreak();
	}

	m_clientSessionState = ClientSessionState::NONE;
	m_serverSessionState = ServerSessionState::NONE;
	m_acceptSessionState = AcceptSessionState::NONE;

	if (m_clientSocket != INVALID_SOCKET)
	{
		__debugbreak();
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
		// 카운터가 음수로 내려갔다.
		// ResetSession / Finalize 가 카운터를 강제로 0 으로 만드는 동안
		// 다른 스레드의 완료 처리가 아직 돌고 있었다는 뜻이다.
		LOGE("session %u IO count went negative (%ld). the counter was reset while a handler was still running",
			GetSessionID(), ioCount);
		__debugbreak();
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
					if ((GetSessionRole() == SESSION_ROLE::CLIENT || GetSessionRole() == SESSION_ROLE::SERVER) && m_ioCancelCompleteEvent)
					{
						::SetEvent(m_ioCancelCompleteEvent);
					}

					// 주의: 여기서 IO 카운트를 확인하지 않고 완료 이벤트를 세운다.
					// CancelIoEx 가 대상을 못 찾았을 뿐 실제로는 완료 대기 중인
					// I/O 가 남아 있을 수 있으므로 카운트도 같이 남긴다.
					LOGI("session %u CancelIoEx found no pending IO (error %lu, io count %ld)",
						GetSessionID(), errorCode, ::InterlockedCompareExchange(&m_ioCount, 0, 0));
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
			__debugbreak();
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
		__debugbreak();
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
