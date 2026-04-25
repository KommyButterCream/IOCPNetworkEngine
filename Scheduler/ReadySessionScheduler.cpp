#include "ReadySessionScheduler.h"

#include "ReadySessionQueue.h"
#include "../Job/Job.h"
#include "../Session/ClientSession.h"
#include "../Session/SessionJobQueue.h"
#include "../Memory/SlabMemoryPoolHelper.h"

#include <process.h> // for _beginthread, _endthread

ReadySessionScheduler::ReadySessionScheduler()
{
}

ReadySessionScheduler::~ReadySessionScheduler()
{
	Finalize();
}

bool ReadySessionScheduler::Initialize(uint32_t workerCount, ReadySessionQueue* readySessionQueue, SlabMemoryPool* jobMemoryPool, SlabMemoryPool* packetMemoryPool, SlabMemoryPool* generalMemoryPool)
{
	if (workerCount == 0 || readySessionQueue == nullptr || jobMemoryPool == nullptr)
	{
		return false;
	}

	m_workerCount = workerCount;
	m_readySessionQueue = readySessionQueue;
	m_jobMemoryPool = jobMemoryPool;
	m_packetMemoryPool = packetMemoryPool;
	m_generalMemoryPool = generalMemoryPool;

	m_stopFlag = 0;
	m_threads = new HANDLE[workerCount];

	if (!m_threads)
		return false;

	for (uint32_t i = 0; i < m_workerCount; i++)
	{
		unsigned int threadId = 0;
		HANDLE thread = (HANDLE)_beginthreadex(
			nullptr,
			0,
			&ReadySessionScheduler::WorkerThreadProc,
			this,
			0,
			&threadId
		);

		if (!thread)
		{
			return false;
		}

		m_threads[i] = thread;
	}

	return true;
}

void ReadySessionScheduler::Finalize()
{
	if (m_threads == nullptr)
		return;

	::InterlockedExchange(&m_stopFlag, 1);

	m_readySessionQueue->WakeAll();

	for (uint32_t i = 0; i < m_workerCount; ++i)
	{
		if (m_threads[i])
		{
			::WaitForSingleObject(m_threads[i], INFINITE);
			::CloseHandle(m_threads[i]);
			m_threads[i] = nullptr;
		}
	}

	delete[] m_threads;
	m_threads = nullptr;

	m_workerCount = 0;
	::InterlockedExchange(&m_stopFlag, 0);
	m_readySessionQueue = nullptr;
	m_jobMemoryPool = nullptr;
	m_packetMemoryPool = nullptr;
	m_generalMemoryPool = nullptr;
}

unsigned int __stdcall ReadySessionScheduler::WorkerThreadProc(LPVOID param)
{
	ReadySessionScheduler* scheduler = static_cast<ReadySessionScheduler*>(param);

	scheduler->WorkerThreadLoop();

	return 0;
}

void ReadySessionScheduler::WorkerThreadLoop()
{
	while (::InterlockedCompareExchange(&m_stopFlag, 0, 0) == 0)
	{
		constexpr const uint32_t timeout_ms = INFINITE;
		ClientSession* session = dynamic_cast<ClientSession*>(m_readySessionQueue->Pop(timeout_ms));
		if (!session)
			continue;

		Job* job = nullptr;

		while (session->GetJobQueue().DequeueJob(job))
		{
			if (job)
			{
				job->Execute();

				if (job->data)
				{
					MEMORY_POOL::ReleasePacket(*m_packetMemoryPool, *m_generalMemoryPool, job->data);
				}

				MEMORY_POOL::ReleaseJob(*m_jobMemoryPool, job);
			}
			else
			{
				__debugbreak();
			}
		}

		session->UpdateProcessingFlag(0);

		if (!session->GetJobQueue().IsEmpty())
		{
			if (session->IsProcessingReady())
			{
				m_readySessionQueue->Push(session);
			}
		}
	}
}
