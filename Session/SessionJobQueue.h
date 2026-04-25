#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>
#include <stdint.h>

#include "SessionDefs.h"

#ifdef BUILD_IOCP_ENGINE_DLL
#define IOCP_ENGINE_API __declspec(dllexport)
#else
#define IOCP_ENGINE_API __declspec(dllimport)
#endif

struct Job;
class SlabMemoryPool;
enum class SESSION_ROLE;

// ���ǳ� JobQueue (singly-linked list) - SRWLock ���� ��ȣ
class IOCP_ENGINE_API SessionJobQueue
{
public:
	SessionJobQueue(SESSION_ROLE sessionRole, SlabMemoryPool* jobMemoryPool, SlabMemoryPool* packetMemoryPool, SlabMemoryPool* generalMemoryPool);
	~SessionJobQueue();

private:
	SESSION_ROLE m_sessionRole = SESSION_ROLE::NONE;

	Job* m_head = nullptr;
	Job* m_tail = nullptr;
	mutable SRWLOCK m_srwLock = SRWLOCK_INIT;
	CONDITION_VARIABLE m_cv = CONDITION_VARIABLE_INIT;
	int32_t m_count = 0;

	SlabMemoryPool* m_jobMemoryPool = nullptr;
	SlabMemoryPool* m_packetMemoryPool = nullptr;
	SlabMemoryPool* m_generalMemoryPool = nullptr;

	bool m_stopFlag = false;

public:
	bool EnqueueJob(Job* job, bool& wasEmpty);
	bool DequeueJob(Job*& outJob);
	void WakeUp();
	bool WaitDequeueJob(Job*& outJob);

	void Reset();

	bool IsEmpty() const;
};
