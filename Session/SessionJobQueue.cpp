#include "SessionJobQueue.h"

#include "ISession.h"
#include "SessionDefs.h"

#include "../Job/Job.h"
#include "../Memory/SlabMemoryPoolHelper.h"


SessionJobQueue::SessionJobQueue(SESSION_ROLE sessionRole, SlabMemoryPool* jobMemoryPool, SlabMemoryPool* packetMemoryPool, SlabMemoryPool* generalMemoryPool)
{
	m_sessionRole = sessionRole;
	m_jobMemoryPool = jobMemoryPool;
	m_packetMemoryPool = packetMemoryPool;
	m_generalMemoryPool = generalMemoryPool;
}

SessionJobQueue::~SessionJobQueue()
{
	Reset();
}

bool SessionJobQueue::EnqueueJob(Job* job, bool& wasEmpty)
{
	if (job == nullptr)
		return false;

	::AcquireSRWLockExclusive(&m_srwLock);

	wasEmpty = (m_head == nullptr);
	job->next = nullptr;

	if (m_tail)
	{
		m_tail->next = job;
		m_tail = job;
	}
	else
	{
		m_head = m_tail = job;
	}

	++m_count;

	if (m_sessionRole == SESSION_ROLE::CLIENT)
	{
		::WakeConditionVariable(&m_cv);
	}

	::ReleaseSRWLockExclusive(&m_srwLock);
	return true;
}

bool SessionJobQueue::DequeueJob(Job*& outJob)
{
	if (m_sessionRole != SESSION_ROLE::SERVER)
	{
		__debugbreak();
	}

	::AcquireSRWLockExclusive(&m_srwLock);

	if (m_head == nullptr)
	{
		outJob = nullptr;
		::ReleaseSRWLockExclusive(&m_srwLock);
		return false;
	}

	outJob = m_head;
	m_head = m_head->next;

	if (m_head == nullptr)
	{
		m_tail = nullptr;
	}

	--m_count;

	::ReleaseSRWLockExclusive(&m_srwLock);
	return true;
}

void SessionJobQueue::WakeUp()
{
	if (m_sessionRole == SESSION_ROLE::CLIENT)
	{
		m_stopFlag = true;
		::WakeConditionVariable(&m_cv);
	}
}

bool SessionJobQueue::WaitDequeueJob(Job*& outJob)
{
	if (m_sessionRole != SESSION_ROLE::CLIENT)
	{
		__debugbreak();
	}

	::AcquireSRWLockExclusive(&m_srwLock);

	while (m_head == nullptr)
	{
		::SleepConditionVariableSRW(&m_cv, &m_srwLock, INFINITE, 0);

		if (m_stopFlag)
		{
			::ReleaseSRWLockExclusive(&m_srwLock);
			return false;
		}
	}

	outJob = m_head;
	m_head = m_head->next;

	if (m_head == nullptr)
	{
		m_tail = nullptr;
	}

	--m_count;

	::ReleaseSRWLockExclusive(&m_srwLock);
	return true;
}

void SessionJobQueue::Reset()
{
	::AcquireSRWLockExclusive(&m_srwLock);

	Job* job = m_head;
	while (job)
	{
		Job* nextJob = job->next;

		if (job->data)
		{
			MEMORY_POOL::ReleasePacket(*m_packetMemoryPool, *m_generalMemoryPool, job->data);
		}

		MEMORY_POOL::ReleaseJob(*m_jobMemoryPool, job);
		job = nextJob;
	}

	m_head = nullptr;
	m_tail = nullptr;
	m_count = 0;
	m_stopFlag = false;

	::ReleaseSRWLockExclusive(&m_srwLock);
}

bool SessionJobQueue::IsEmpty() const
{
	::AcquireSRWLockShared(&m_srwLock);
	bool isEmpty = (m_count == 0);
	::ReleaseSRWLockShared(&m_srwLock);
	return isEmpty;
}
