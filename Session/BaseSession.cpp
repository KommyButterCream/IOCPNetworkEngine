#include "BaseSession.h"

#include "SessionDefs.h"

#include "../Modules/Core/Util/Logger.h"

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
		__debugbreak();
	}

	if (::InterlockedCompareExchange(&m_cancelIo, 0, 0) == 1 && ioCount == 0 && m_ioCancelCompleteEvent)
	{
		Logger::Log(LogLevel::LOG_INFO, "[%s][BaseSession : %d] all pending IO cancelled", __FUNCTION__, GetSessionID());
		::SetEvent(m_ioCancelCompleteEvent);
	}
}

bool BaseSession::CancelPendingIO()
{
	if (m_clientSocket != INVALID_SOCKET)
	{
		if (::InterlockedExchange(&m_cancelIo, 1) == 0)
		{
			Logger::Log(LogLevel::LOG_INFO, "[%s][BaseSession : %d] CancelIoEx begin", __FUNCTION__, GetSessionID());

			if (!::CancelIoEx(reinterpret_cast<HANDLE>(m_clientSocket), nullptr))
			{
				const DWORD errorCode = ::GetLastError();

				if (errorCode == ERROR_NOT_FOUND || errorCode == ERROR_INVALID_HANDLE)
				{
					if ((GetSessionRole() == SESSION_ROLE::CLIENT || GetSessionRole() == SESSION_ROLE::SERVER) && m_ioCancelCompleteEvent)
					{
						::SetEvent(m_ioCancelCompleteEvent);
					}

					Logger::Log(LogLevel::LOG_INFO, "[%s][BaseSession : %d] CancelIoEx no pending IO", __FUNCTION__, GetSessionID());
					return true;
				}
				else if (errorCode == ERROR_OPERATION_ABORTED)
				{
					Logger::Log(LogLevel::LOG_INFO, "[%s][BaseSession : %d] CancelIoEx returned aborted", __FUNCTION__, GetSessionID());
					return false;
				}
				else
				{
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
			Logger::Log(LogLevel::LOG_INFO, "[%s][BaseSession : %d] IO cancel completed", __FUNCTION__, GetSessionID());
			return true;
		case WAIT_TIMEOUT:
			Logger::Log(LogLevel::LOG_INFO, "[%s][BaseSession : %d] IO cancel timeout", __FUNCTION__, GetSessionID());
			return false;
		default:
			Logger::Log(LogLevel::LOG_INFO, "[%s][BaseSession : %d] unhandled wait result", __FUNCTION__, GetSessionID());
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
			Logger::Log(LogLevel::LOG_WARNING, "[%s][BaseSession : %d] socket was not detached before disconnect (%d)", __FUNCTION__, GetSessionID(), static_cast<int>(m_clientSocket));
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

		Logger::Log(LogLevel::LOG_INFO, "[%s][BaseSession : %d] disconnected", __FUNCTION__, GetSessionID());
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
