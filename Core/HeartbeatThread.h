#pragma once

#include <stdint.h>

#include "../../CoreLib/ThreadBase.h"

class SessionManager;

class HeartbeatThread final : public CoreLibrary::Concurrency::ThreadBase
{
public:
	HeartbeatThread(SessionManager* sessionManager, uint64_t checkInterval_ms, uint64_t heartbeatTimeout_ms);
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
};
