#include "SendPacketQueue.h"

#include <assert.h> // for assert
#include <malloc.h> // for _aligned_malloc, _aligned_free

#include "SendPacketPool.h"

#include "../Memory/SlabMemoryPoolHelper.h"

namespace
{
	void ReleaseQueuedPacket(SlabMemoryPool& packetMemoryPool, SlabMemoryPool& generalMemoryPool, SendPacketBuffer* packetBuffer)
	{
		if (!packetBuffer || !packetBuffer->packetData)
			return;

		if (packetBuffer->releaseFunc)
		{
			packetBuffer->releaseFunc(packetBuffer->packetData, packetBuffer->releaseContext);
		}
		else
		{
			MEMORY_POOL::ReleasePacket(packetMemoryPool, generalMemoryPool, packetBuffer->packetData);
		}

		packetBuffer->Reset();
	}
}

SendPacketQueue::SendPacketQueue()
{
}

bool SendPacketQueue::Initialize(SendPacketPool* sendPacketPool, SlabMemoryPool* packetMemoryPool, SlabMemoryPool* generalMemoryPool)
{
	static_assert((SEND_PACKET_QUEUE_SIZE & (SEND_PACKET_QUEUE_SIZE - 1)) == 0, "Buffer size must be power of 2");

	if (!sendPacketPool || !packetMemoryPool || !generalMemoryPool)
	{
		return false;
	}

	Finalize();

	m_packetPool = sendPacketPool;
	m_packetMemoryPool = packetMemoryPool;
	m_generalMemoryPool = generalMemoryPool;
	m_queue = new SendPacketBuffer * [SEND_PACKET_QUEUE_SIZE] {};
	if (!m_queue)
	{
		Finalize();
		return false;
	}

	m_head = 0;
	m_tail = 0;
	m_count = 0;

	return true;
}

SendPacketQueue::~SendPacketQueue()
{
	Finalize();
}

void SendPacketQueue::Finalize()
{
	Reset();

	if (m_queue)
	{
		delete[] m_queue;
		m_queue = nullptr;
	}

	m_packetMemoryPool = nullptr;
	m_generalMemoryPool = nullptr;
	m_packetPool = nullptr;
	m_head = 0;
	m_tail = 0;
	m_count = 0;
}

bool SendPacketQueue::Enqueue(void** packetData, uint32_t packetSize)
{
	if (!m_queue || !m_packetPool || !m_packetMemoryPool || !m_generalMemoryPool)
		return false;

	if (!packetData || !(*packetData) || packetSize == 0 || packetSize > MEMORY_SIZE_32K)
		return false;

	::AcquireSRWLockExclusive(&m_srwLock);

	if (m_count >= SEND_PACKET_QUEUE_SIZE)
	{
		::ReleaseSRWLockExclusive(&m_srwLock);

		return false;
	}

	SendPacketBuffer* block = m_packetPool->Acquire();
	if (!block)
	{
		::ReleaseSRWLockExclusive(&m_srwLock);

		return false;
	}

	// 주의
	// 내용을 카피 하지 않고
	// 메모리 풀의 주소 자체를 복사한다.
	// 복사 횟수를 최대한 줄이기 위해
	// 대신 packetData 에 nullptr 로 설정해주어서
	// Scheduler 의 DestoryPacket 에서 메모리 풀 반환을 안하도록 함!
	// 소유권을 넘겨받은 메모리 풀 해제는 WSASend Complete 에서 전송이 완료되면 수행하도록 한다.
	block->packetData = static_cast<const char*>(*packetData);
	*packetData = nullptr;

	block->packetSize = packetSize;
	block->releaseFunc = nullptr;
	block->releaseContext = nullptr;

	m_queue[m_tail] = block;
	m_tail = (m_tail + 1) & (SEND_PACKET_QUEUE_SIZE - 1);
	++m_count;

	::ReleaseSRWLockExclusive(&m_srwLock);

	return true;
}

bool SendPacketQueue::EnqueueShared(const void* packetData, uint32_t packetSize, SendPacketReleaseFunc releaseFunc, void* releaseContext)
{
	if (!m_queue || !m_packetPool || !m_packetMemoryPool || !m_generalMemoryPool)
		return false;

	if (!packetData || packetSize == 0 || packetSize > MEMORY_SIZE_32K || !releaseFunc)
		return false;

	::AcquireSRWLockExclusive(&m_srwLock);

	if (m_count >= SEND_PACKET_QUEUE_SIZE)
	{
		::ReleaseSRWLockExclusive(&m_srwLock);

		return false;
	}

	SendPacketBuffer* block = m_packetPool->Acquire();
	if (!block)
	{
		::ReleaseSRWLockExclusive(&m_srwLock);

		return false;
	}

	block->packetData = static_cast<const char*>(packetData);
	block->packetSize = packetSize;
	block->releaseFunc = releaseFunc;
	block->releaseContext = releaseContext;

	m_queue[m_tail] = block;
	m_tail = (m_tail + 1) & (SEND_PACKET_QUEUE_SIZE - 1);
	++m_count;

	::ReleaseSRWLockExclusive(&m_srwLock);

	return true;
}

bool SendPacketQueue::Dequeue(SendPacketBuffer*& outBlock)
{
	outBlock = nullptr;

	if (!m_queue)
		return false;

	::AcquireSRWLockExclusive(&m_srwLock);

	if (IsEmpty())
	{
		::ReleaseSRWLockExclusive(&m_srwLock);

		return false;
	}

	outBlock = m_queue[m_head];
	m_queue[m_head] = nullptr;
	m_head = (m_head + 1) & (SEND_PACKET_QUEUE_SIZE - 1);
	--m_count;

	::ReleaseSRWLockExclusive(&m_srwLock);

	return true;
}

void SendPacketQueue::Reset()
{
	if (!m_queue || !m_packetPool || !m_packetMemoryPool || !m_generalMemoryPool)
		return;

	::AcquireSRWLockExclusive(&m_srwLock);

	// 남아 있는 블록 반환
	for (int i = 0; i < SEND_PACKET_QUEUE_SIZE; ++i)
	{
		if (m_queue[i])
		{
			ReleaseQueuedPacket(*m_packetMemoryPool, *m_generalMemoryPool, m_queue[i]);

			m_packetPool->Release(m_queue[i]);
			m_queue[i] = nullptr;
		}
	}

	m_head = 0;
	m_tail = 0;
	m_count = 0;

	::ReleaseSRWLockExclusive(&m_srwLock);
}

bool SendPacketQueue::IsEmpty() const
{
	return m_count == 0;
}
