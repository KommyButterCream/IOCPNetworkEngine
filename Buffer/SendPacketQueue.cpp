#include "SendPacketQueue.h"

#include <assert.h> // for assert
#include <malloc.h> // for _aligned_malloc, _aligned_free

#include "SendPacketPool.h"

#include "../Diagnostics/EngineAssert.h"

#include "../Memory/EngineMemoryPoolHelper.h"

namespace
{
	void ReleaseQueuedPacket(EngineMemoryPool& packetMemoryPool, EngineMemoryPool& generalMemoryPool, SendPacketBuffer* packetBuffer)
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

bool SendPacketQueue::Initialize(SendPacketPool* sendPacketPool, EngineMemoryPool* packetMemoryPool, EngineMemoryPool* generalMemoryPool, uint32_t depth)
{
	if (!sendPacketPool || !packetMemoryPool || !generalMemoryPool)
	{
		return false;
	}

	// 예전에는 static_assert 였다. 깊이가 런타임 값이 되었으므로 여기서 막는다.
	if (depth == 0 || (depth & (depth - 1)) != 0)
	{
		ENGINE_VIOLATION("send queue depth %u is not a power of two", depth);
		return false;
	}

	Finalize();

	m_packetPool = sendPacketPool;
	m_packetMemoryPool = packetMemoryPool;
	m_generalMemoryPool = generalMemoryPool;
	m_capacity = static_cast<int32_t>(depth);
	m_capacityMask = m_capacity - 1;
	m_queue = new SendPacketBuffer * [depth] {};
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
	m_capacity = 0;
	m_capacityMask = 0;
}

bool SendPacketQueue::Enqueue(void** packetData, uint32_t packetSize)
{
	if (!m_queue || !m_packetPool || !m_packetMemoryPool || !m_generalMemoryPool)
		return false;

	if (!packetData || !(*packetData) || packetSize == 0 || packetSize > PACKET_SIZE_LIMIT)
		return false;

	// 풀 획득을 락 밖에서 먼저 한다.
	//
	// 예전에는 락을 잡은 채 Acquire 를 불렀다. 고갈 시 그 안에서 LOGW 까지
	// 나가므로 "큐 락 -> 로거 락" 순서가 생기고, 락 보유 시간도 SList pop
	// 만큼 길어진다. 자리가 없으면 방금 얻은 블록을 되돌려주면 된다.
	SendPacketBuffer* block = m_packetPool->Acquire();
	if (!block)
		return false;

	::AcquireSRWLockExclusive(&m_srwLock);

	if (m_count >= m_capacity)
	{
		::ReleaseSRWLockExclusive(&m_srwLock);

		m_packetPool->Release(block);
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
	m_tail = (m_tail + 1) & (m_capacityMask);
	++m_count;

	::ReleaseSRWLockExclusive(&m_srwLock);

	return true;
}

bool SendPacketQueue::EnqueueShared(const void* packetData, uint32_t packetSize, SendPacketReleaseFunc releaseFunc, void* releaseContext)
{
	if (!m_queue || !m_packetPool || !m_packetMemoryPool || !m_generalMemoryPool)
		return false;

	if (!packetData || packetSize == 0 || packetSize > PACKET_SIZE_LIMIT || !releaseFunc)
		return false;

	// 풀 획득을 락 밖에서 먼저 한다.
	//
	// 예전에는 락을 잡은 채 Acquire 를 불렀다. 고갈 시 그 안에서 LOGW 까지
	// 나가므로 "큐 락 -> 로거 락" 순서가 생기고, 락 보유 시간도 SList pop
	// 만큼 길어진다. 자리가 없으면 방금 얻은 블록을 되돌려주면 된다.
	SendPacketBuffer* block = m_packetPool->Acquire();
	if (!block)
		return false;

	::AcquireSRWLockExclusive(&m_srwLock);

	if (m_count >= m_capacity)
	{
		::ReleaseSRWLockExclusive(&m_srwLock);

		m_packetPool->Release(block);
		return false;
	}

	block->packetData = static_cast<const char*>(packetData);
	block->packetSize = packetSize;
	block->releaseFunc = releaseFunc;
	block->releaseContext = releaseContext;

	m_queue[m_tail] = block;
	m_tail = (m_tail + 1) & (m_capacityMask);
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

	if (IsEmptyLocked())
	{
		::ReleaseSRWLockExclusive(&m_srwLock);

		return false;
	}

	outBlock = m_queue[m_head];
	m_queue[m_head] = nullptr;
	m_head = (m_head + 1) & (m_capacityMask);
	--m_count;

	::ReleaseSRWLockExclusive(&m_srwLock);

	return true;
}

void SendPacketQueue::Reset()
{
	if (!m_queue || !m_packetPool || !m_packetMemoryPool || !m_generalMemoryPool)
		return;

	::AcquireSRWLockExclusive(&m_srwLock);

	// 남아 있는 블록 반환.
	//
	// 예전에는 용량 전체를 훑었다. 담긴 것은 head 부터 m_count 개뿐이고
	// 나머지는 항상 nullptr 이다. depth 4096 인 세션을 정리할 때마다 4096회를
	// 돌았고, disconnect storm 에서는 세션 수만큼 곱해졌다.
	for (int32_t i = 0; i < m_count; ++i)
	{
		const int32_t index = (m_head + i) & m_capacityMask;

		if (!m_queue[index])
		{
			// head..head+count 구간은 반드시 채워져 있어야 한다.
			ENGINE_VIOLATION("send queue slot %d is empty inside head..count (head %d, count %d)",
				index, m_head, m_count);
			continue;
		}

		ReleaseQueuedPacket(*m_packetMemoryPool, *m_generalMemoryPool, m_queue[index]);

		m_packetPool->Release(m_queue[index]);
		m_queue[index] = nullptr;
	}

	m_head = 0;
	m_tail = 0;
	m_count = 0;

	::ReleaseSRWLockExclusive(&m_srwLock);
}

bool SendPacketQueue::IsEmpty() const
{
	// m_count 는 exclusive 락 아래에서만 변경되므로 shared 락으로 충분하다.
	::AcquireSRWLockShared(&m_srwLock);
	const bool empty = (m_count == 0);
	::ReleaseSRWLockShared(&m_srwLock);

	return empty;
}

bool SendPacketQueue::IsEmptyLocked() const
{
	return m_count == 0;
}
