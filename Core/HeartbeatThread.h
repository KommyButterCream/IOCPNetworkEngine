#pragma once

#include <stdint.h>

#include "../../Core/Concurrency/ThreadBase.h"

class SessionManager;

// 주기 점검에서 함께 돌릴 일을 받는다. HeartbeatThread 가 IOCPServer 를
// 알지 않아도 되도록 함수 포인터로 둔다. (엔진이 CloseSocketFunc 등에서
// 쓰는 방식과 같다)
typedef void (*PeriodicMaintenanceFunc)(void* context);

class HeartbeatThread final : public Core::Concurrency::ThreadBase
{
public:
	HeartbeatThread(SessionManager* sessionManager, uint64_t checkInterval_ms, uint64_t heartbeatTimeout_ms,
		PeriodicMaintenanceFunc maintenanceFunc = nullptr, void* maintenanceContext = nullptr);
	~HeartbeatThread() override = default;

	HeartbeatThread(const HeartbeatThread&) = delete;
	HeartbeatThread& operator=(const HeartbeatThread&) = delete;

	void SetCheckInterval(uint64_t checkInterval_ms);
	void SetHeartbeatTimeout(uint64_t heartbeatTimeout_ms);

protected:
	void Run() override;

private:
	SessionManager* m_sessionManager = nullptr;
	uint64_t m_checkInterval_ms = 0;
	uint64_t m_heartbeatTimeout_ms = 0;

	PeriodicMaintenanceFunc m_maintenanceFunc = nullptr;
	void* m_maintenanceContext = nullptr;
};
