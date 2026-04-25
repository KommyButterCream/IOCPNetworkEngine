#include "ClientSessionScheduler.h"

#include "../Job/Job.h"
#include "../../Session/ClientSession.h"
#include "../../Session/SessionJobQueue.h"
#include "../../Memory/SlabMemoryPoolHelper.h"

#include <process.h> // for _beginthread, _endthread

ClientSessionScheduler::ClientSessionScheduler()
{
}

ClientSessionScheduler::~ClientSessionScheduler()
{
	Finalize();
}

bool ClientSessionScheduler::Initialize(ClientSession* clientSession, SlabMemoryPool* jobMemoryPool, SlabMemoryPool* packetMemoryPool, SlabMemoryPool* generalMemoryPool)
{
	if (clientSession == nullptr || jobMemoryPool == nullptr)
	{
		return false;
	}

	m_clientSession = clientSession;
	m_jobMemoryPool = jobMemoryPool;
	m_packetMemoryPool = packetMemoryPool;
	m_generalMemoryPool = generalMemoryPool;

	::InterlockedExchange(&m_stopFlag, 0);

	m_stopEvent = ::CreateEvent(nullptr, TRUE, FALSE, nullptr);
	if (!m_stopEvent)
		return false;

	unsigned int threadId = 0;
	HANDLE thread = (HANDLE)_beginthreadex(
		nullptr,
		0,
		&ClientSessionScheduler::WorkerThreadProc,
		this,
		0,
		&threadId
	);

	if (!thread)
	{
		return false;
	}

	m_thread = thread;

	return true;
}

void ClientSessionScheduler::Finalize()
{
	if (m_thread == nullptr)
		return;

	::InterlockedExchange(&m_stopFlag, 1);

	if (m_stopEvent)
	{
		::SetEvent(m_stopEvent);
	}

	if (m_clientSession)
	{
		m_clientSession->GetJobQueue().WakeUp();
	}

	if (m_thread)
	{
		::WaitForSingleObject(m_thread, INFINITE);
		::CloseHandle(m_thread);
		m_thread = nullptr;
	}

	if (m_stopEvent)
	{
		::ResetEvent(m_stopEvent);
		::CloseHandle(m_stopEvent);
		m_stopEvent = nullptr;
	}

	::InterlockedExchange(&m_stopFlag, 0);

	m_jobMemoryPool = nullptr;
	m_clientSession = nullptr;
	m_generalMemoryPool = nullptr;
	m_packetMemoryPool = nullptr;
}

unsigned int __stdcall ClientSessionScheduler::WorkerThreadProc(LPVOID param)
{
	ClientSessionScheduler* scheduler = static_cast<ClientSessionScheduler*>(param);

	scheduler->WorkerThreadLoop();

	return 0;
}

void ClientSessionScheduler::WorkerThreadLoop()
{
	while (::InterlockedCompareExchange(&m_stopFlag, 0, 0) == 0)
	{
		Job* job = nullptr;
		bool gotJob = m_clientSession->GetJobQueue().WaitDequeueJob(job);

		if (!gotJob)
			break;

		if (job)
		{
			m_clientSession->SetCurrentJob(job);
			job->Execute();

			if (job->jobType == JobType::PACKET)
			{
				if (job->data)
				{
					MEMORY_POOL::ReleasePacket(*m_packetMemoryPool, *m_generalMemoryPool, job->data);
				}
			}

			MEMORY_POOL::ReleaseJob(*m_jobMemoryPool, job);
		}

		m_clientSession->UpdateProcessingFlag(0);
	}
}
