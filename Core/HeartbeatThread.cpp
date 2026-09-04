#include "HeartbeatThread.h"

#include <Windows.h>

#include "../../Core/Util/Logger.h"
#include "../Session/SessionManager.h"

using namespace Core::Util;

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

		// 이전 코드는 heartbeatRequestCount 대신 m_heartbeatTimeout_ms 를 찍고 있었다.
		// (uint64_t 를 %u 로 넘겨서 가변인자 UB 이기도 했다)
		const uint32_t heartbeatRequestCount = m_sessionManager->SendHeartbeatRequests();
		if (heartbeatRequestCount > 0)
		{
			LOGI("sent %u heartbeat requests", heartbeatRequestCount);
		}

		const uint32_t disconnectedCount = m_sessionManager->DisconnectZombieSessions(m_heartbeatTimeout_ms);
		if (disconnectedCount > 0)
		{
			LOGW("disconnected %u zombie sessions (timeout %llu ms)", disconnectedCount, m_heartbeatTimeout_ms);
		}
	}
}
