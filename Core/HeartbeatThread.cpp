#include "HeartbeatThread.h"

#include <Windows.h>

#include "../Diagnostics/EngineAssert.h"

#include "../../Core/Util/Logger.h"
#include "../Session/SessionManager.h"

using namespace Core::Util;

HeartbeatThread::HeartbeatThread(SessionManager* sessionManager, uint64_t checkInterval_ms, uint64_t heartbeatTimeout_ms,
	PeriodicMaintenanceFunc maintenanceFunc, void* maintenanceContext)
	: ThreadBase(L"HeartbeatThread")
{
	m_sessionManager = sessionManager;
	m_checkInterval_ms = checkInterval_ms;
	m_heartbeatTimeout_ms = heartbeatTimeout_ms;
	m_maintenanceFunc = maintenanceFunc;
	m_maintenanceContext = maintenanceContext;
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
			ENGINE_VIOLATION("heartbeat wait returned %lu (error %lu), stopping the thread", waitResult, ::GetLastError());
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

		// 주기 점검에 얹은 일. 지금은 비어 버린 accept 슬롯 보충이 여기로
		// 온다. 이 스레드가 단일이라는 점이 그 작업의 안전 조건이다.
		if (m_maintenanceFunc)
		{
			m_maintenanceFunc(m_maintenanceContext);
		}
	}
}
