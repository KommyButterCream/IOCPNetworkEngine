#include "AcceptSession.h"

#include <WinSock2.h>

#include "../../Core/Util/Logger.h"

using namespace Core::Util;

#pragma comment(lib, "ws2_32.lib")

AcceptSession::AcceptSession()
{
	::ZeroMemory(&m_acceptBuffer, sizeof(m_acceptBuffer));
	::ZeroMemory(&m_acceptOverlapped, sizeof(OverlappedEx));
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
	::ZeroMemory(&m_acceptOverlapped, sizeof(OverlappedEx));
	m_acceptOverlapped.operation = IO_OPERATION::ACCEPT;
}

void AcceptSession::Finalize()
{
	if (m_destroyFlag)
	{
		return;
	}

	BaseSession::Finalize();

	::ZeroMemory(&m_acceptBuffer, sizeof(m_acceptBuffer));
	::ZeroMemory(&m_acceptOverlapped, sizeof(OverlappedEx));
	m_acceptOverlapped.operation = IO_OPERATION::ACCEPT;

	m_destroyFlag = true;
}

bool AcceptSession::OnAccept()
{
	SetAcceptSessionState(AcceptSessionState::ACCEPT_COMPLETE);
	Logger::Log(LogLevel::LOG_INFO, "[%s][AcceptSession : %d] accept complete", __FUNCTION__, GetSessionID());
	return true;
}

bool AcceptSession::OnConnect()
{
	return false;
}

bool AcceptSession::OnDisconnect()
{
	if (::InterlockedExchange(&m_closing, 1) == 0)
	{
		Logger::Log(LogLevel::LOG_INFO, "[%s][AcceptSession : %d] begin disconnect socket(%d)", __FUNCTION__, GetSessionID(), (int)GetClientSocket());

		if (!IsSocketInvalid())
		{
			Logger::Log(LogLevel::LOG_WARNING, "[%s][AcceptSession : %d] socket was not detached before disconnect (%d)", __FUNCTION__, GetSessionID(), static_cast<int>(GetClientSocket()));
			__debugbreak();
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
