#include "ReadySessionScheduler.h"

#include <stdio.h> // for swprintf_s
#include <new>     // for std::nothrow

#include "ReadySessionQueue.h"
#include "../Job/Job.h"
#include "../Session/ClientSession.h"
#include "../Session/SessionJobQueue.h"
#include "../Memory/EngineMemoryPoolHelper.h"

#include "../../Core/Util/Logger.h"

using namespace Core::Util;

void ReadySessionWorker::Run()
{
	if (m_owner)
	{
		m_owner->WorkerThreadLoop(*this, m_workerIndex);
	}
}

ReadySessionScheduler::ReadySessionScheduler()
{
}

ReadySessionScheduler::~ReadySessionScheduler()
{
	Finalize();
}

bool ReadySessionScheduler::Initialize(uint32_t workerCount, ReadySessionQueue* readySessionQueue, EngineMemoryPool* jobMemoryPool, EngineMemoryPool* packetMemoryPool, EngineMemoryPool* generalMemoryPool)
{
	if (workerCount == 0 || readySessionQueue == nullptr || jobMemoryPool == nullptr)
	{
		LOGE("invalid arguments (workerCount %u, queue %p, jobPool %p)", workerCount, readySessionQueue, jobMemoryPool);
		return false;
	}

	m_readySessionQueue = readySessionQueue;
	m_jobMemoryPool = jobMemoryPool;
	m_packetMemoryPool = packetMemoryPool;
	m_generalMemoryPool = generalMemoryPool;

	m_workers = static_cast<ReadySessionWorker**>(
		::HeapAlloc(::GetProcessHeap(), HEAP_ZERO_MEMORY, sizeof(ReadySessionWorker*) * workerCount));
	if (!m_workers)
	{
		LOGE("failed to allocate the worker table (count %u)", workerCount);
		return false;
	}

	m_workerCount = workerCount;

	for (uint32_t i = 0; i < m_workerCount; ++i)
	{
		// 스레드에 이름을 붙여 두면 디버거와 ETW 추적에서 바로 식별된다.
		wchar_t threadName[64] = {};
		::swprintf_s(threadName, L"ReadySessionWorker-%u", i);

		m_workers[i] = new (std::nothrow) ReadySessionWorker(threadName);
		if (!m_workers[i])
		{
			LOGE("failed to create worker %u", i);
			DestroyWorkers();
			return false;
		}

		m_workers[i]->Bind(this, i);

		if (!m_workers[i]->Start())
		{
			LOGE("failed to start worker %u", i);
			DestroyWorkers();
			return false;
		}
	}

	LOGI("ready session scheduler started with %u workers", m_workerCount);

	return true;
}

void ReadySessionScheduler::Finalize()
{
	if (m_workers == nullptr)
		return;

	// 정지 요청을 먼저 보낸다.
	// 워커는 ReadySessionQueue::Pop 에서 조건 변수로 블로킹되므로
	// 정지 이벤트만으로는 깨어나지 않는다. WakeAll 로 깨워야 조인이 성립한다.
	for (uint32_t i = 0; i < m_workerCount; ++i)
	{
		if (m_workers[i]) m_workers[i]->RequestStop();
	}

	if (m_readySessionQueue)
	{
		m_readySessionQueue->WakeAll();
	}

	DestroyWorkers();

	LOGI("ready session scheduler stopped");

	m_readySessionQueue = nullptr;
	m_jobMemoryPool = nullptr;
	m_packetMemoryPool = nullptr;
	m_generalMemoryPool = nullptr;
}

void ReadySessionScheduler::DestroyWorkers()
{
	if (!m_workers)
		return;

	for (uint32_t i = 0; i < m_workerCount; ++i)
	{
		if (m_workers[i])
		{
			// 소멸자가 Stop()(정지 요청 + 조인) 을 수행한다.
			delete m_workers[i];
			m_workers[i] = nullptr;
		}
	}

	::HeapFree(::GetProcessHeap(), 0, m_workers);
	m_workers = nullptr;
	m_workerCount = 0;
}

void ReadySessionScheduler::WorkerThreadLoop(ReadySessionWorker& worker, uint32_t workerIndex)
{
	LOGI("worker %u entering loop", workerIndex);

	while (!worker.IsStopRequested())
	{
		constexpr uint32_t timeout_ms = INFINITE;

		// 큐에는 ClientSession 만 들어가므로 dynamic_cast 가 필요하지 않다.
		ISession* popped = m_readySessionQueue->Pop(timeout_ms);
		if (!popped)
			continue;

		ClientSession* session = static_cast<ClientSession*>(popped);

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
				LOGE("DequeueJob reported success but returned a null job (session %u)", session->GetSessionID());
				break;
			}
		}

		session->UpdateProcessingFlag(0);

		// 드레인 중에 새로 들어온 Job 이 있으면 다시 큐에 올린다.
		if (!session->GetJobQueue().IsEmpty())
		{
			if (session->IsProcessingReady())
			{
				if (!m_readySessionQueue->Push(session))
				{
					// Push 실패 시 소유권 플래그를 되돌려야 세션이 영구히 묶이지 않는다.
					session->UpdateProcessingFlag(0);
					LOGE("failed to re-queue session %u : ready queue is full", session->GetSessionID());
				}
			}
		}
	}

	LOGI("worker %u leaving loop", workerIndex);
}
