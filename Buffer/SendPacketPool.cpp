#include "SendPacketPool.h"

#include <assert.h> // for assert
#include <malloc.h> // for _aligned_malloc, _aligned_free
#include <new> // for placement-new, delete

#include "../../Core/Util/Logger.h"

using namespace Core::Util;

SendPacketPool::SendPacketPool()
{
	::InitializeSListHead(&m_sListHead);
}

bool SendPacketPool::Initialize(uint32_t blockCount)
{
	Finalize();

	m_blockCount = blockCount;
	if (m_blockCount == 0)
	{
		return false;
	}

	size_t totalSize = sizeof(SendPacketBuffer) * static_cast<size_t>(m_blockCount);
	m_bufferPool = static_cast<SendPacketBuffer*>(::_aligned_malloc(totalSize, 64));

	if (m_bufferPool == nullptr)
	{
		Finalize();
		return false;
	}

	for (uint32_t i = 0; i < m_blockCount; ++i)
	{
		new (&m_bufferPool[i]) SendPacketBuffer();

		::InterlockedPushEntrySList(&m_sListHead, static_cast<PSLIST_ENTRY>(&m_bufferPool[i]));
	}

	return true;
}

SendPacketPool::~SendPacketPool()
{
	Finalize();
}

void SendPacketPool::Finalize()
{
	if (m_bufferPool)
	{
		::InterlockedFlushSList(&m_sListHead);

		for (uint32_t i = 0; i < m_blockCount; ++i)
		{
			m_bufferPool[i].~SendPacketBuffer();
		}

		::_aligned_free(m_bufferPool);
		m_bufferPool = nullptr;
	}

	m_blockCount = 0;
	m_acqCount = 0;
	m_relCount = 0;
	::InitializeSListHead(&m_sListHead);
}

SendPacketBuffer* SendPacketPool::Acquire()
{
	if (!m_bufferPool)
	{
		return nullptr;
	}

	PSLIST_ENTRY pEntry = ::InterlockedPopEntrySList(&m_sListHead);

	if (!pEntry)
	{
		// 풀 고갈. 지금까지는 조용히 nullptr 을 반환해서
		// 상위(SendPacketQueue::Enqueue)에서 원인 구분이 불가능했다.
		// 이 풀은 세션이 공유하므로(HybridSendPacketPool 샤딩) 고갈은
		// 곧 해당 샤드의 송신 실패를 뜻한다.
		LOGW("send packet pool exhausted (block count %u). sends on this shard will fail", m_blockCount);
		return nullptr;
	}

	SendPacketBuffer* pPacketBuffer = static_cast<SendPacketBuffer*>(pEntry);

	pPacketBuffer->Reset();

	::InterlockedIncrement64(reinterpret_cast<long long*>(&m_acqCount));

	uint64_t currentAcq = ::InterlockedCompareExchange64(reinterpret_cast<long long*>(&m_acqCount), 0, 0);
	//printf_s("ACQ[%d] : %p\n", currentAcq, pPacketBuffer);

	return pPacketBuffer;
}

void SendPacketPool::Release(SendPacketBuffer* pPacketBuffer)
{
	if (!m_bufferPool || !pPacketBuffer)
		return;

	::InterlockedPushEntrySList(&m_sListHead, static_cast<PSLIST_ENTRY>(pPacketBuffer));

	::InterlockedIncrement64(reinterpret_cast<long long*>(&m_relCount));

	uint64_t currentRel = ::InterlockedCompareExchange64(reinterpret_cast<long long*>(&m_relCount), 0, 0);
	//printf_s("REL[%d] : %p\n", currentRel, pPacketBuffer);
}
