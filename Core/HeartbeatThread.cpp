#include "HeartbeatThread.h"

#include <Windows.h>

#include "../../CoreLib/Utils/Logger.h"
#include "../Session/SessionManager.h"

using namespace CoreLibrary::Utils;

HeartbeatThread::HeartbeatThread(SessionManager* sessionManager, uint64_t checkInterval_ms, uint64_t heartbeatTimeout_ms)
	: ThreadBase(L"HeartbeatThread")
{
	m_sessionManager = sessionManager;
	m_checkInterval_ms = checkInterval_ms;
	m_heartbeatTimeout_ms = heartbeatTimeout_ms;
}

void HeartbeatThread::SetCheckInterval(uint64_t checkInterval_ms)
{
	m_checkInterval_ms = checkInterval_ms;
}

void HeartbeatThread::SetHeartbeatTimeout(uint64_t heartbeatTimeout_ms)
{
	m_heartbeatTimeout_ms = heartbeatTimeout_ms;
}

void HeartbeatThread::Run()
{
	const DWORD waitInterval_ms = (m_checkInterval_ms > static_cast<uint64_t>(MAXDWORD))
		? MAXDWORD : static_cast<DWORD>(m_checkInterval_ms);

	while (!IsStopRequested())
	{
		const DWORD waitResult = ::WaitForSingleObject(GetStopEvent(), waitInterval_ms);
		if (waitResult == WAIT_OBJECT_0)
		{
			break;
		}

		if (waitResult != WAIT_TIMEOUT)
		{
			__debugbreak();
			break;
		}

		if (!m_sessionManager)
		{
			continue;
		}

		const uint32_t heartbeatRequestCount = m_sessionManager->SendHeartbeatRequests();
		if (m_heartbeatTimeout_ms > 0)
		{
			Logger::Log(LogLevel::LOG_INFO, "[%s] sent heartbeat requests: %u", __FUNCTION__, m_heartbeatTimeout_ms);
		}

		const uint32_t disconnectedCount = m_sessionManager->DisconnectZombieSessions(m_heartbeatTimeout_ms);
		if (disconnectedCount > 0)
		{
			Logger::Log(LogLevel::LOG_WARNING, "[%s] disconnected zombie sessions: %u", __FUNCTION__, disconnectedCount);
		}
	}
}
