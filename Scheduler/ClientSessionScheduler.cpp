#include "ClientSessionScheduler.h"

#include "../Job/Job.h"
#include "../Session/ClientSession.h"
#include "../Session/SessionJobQueue.h"
#include "../Memory/SlabMemoryPoolHelper.h"

#include "../../Core/Util/Logger.h"

using namespace Core::Util;

ClientSessionScheduler::ClientSessionScheduler()
	: ThreadBase(L"ClientSessionScheduler")
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
		LOGE("invalid arguments (session %p, jobPool %p)", clientSession, jobMemoryPool);
		return false;
	}

	m_clientSession = clientSession;
	m_jobMemoryPool = jobMemoryPool;
	m_packetMemoryPool = packetMemoryPool;
	m_generalMemoryPool = generalMemoryPool;

	if (!Start())
	{
		LOGE("failed to start the scheduler thread");
		return false;
	}

	LOGI("client session scheduler started (thread id %u)", GetThreadId());

	return true;
}

void ClientSessionScheduler::Finalize()
{
	if (!IsRunning() && m_clientSession == nullptr)
	{
		return;
	}

	// 정지 요청만 먼저 보낸다.
	// Run() 은 SessionJobQueue::WaitDequeueJob 에서 조건 변수로 블로킹되므로
	// 정지 이벤트만으로는 깨어나지 않는다. WakeUp 으로 깨워야 조인이 성립한다.
	RequestStop();

	if (m_clientSession)
	{
		m_clientSession->GetJobQueue().WakeUp();
	}

	Join();

	LOGI("client session scheduler stopped");

	m_jobMemoryPool = nullptr;
	m_clientSession = nullptr;
	m_generalMemoryPool = nullptr;
	m_packetMemoryPool = nullptr;
}

void ClientSessionScheduler::Run()
{
	while (!IsStopRequested())
	{
		Job* job = nullptr;
		const bool gotJob = m_clientSession->GetJobQueue().WaitDequeueJob(job);

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
