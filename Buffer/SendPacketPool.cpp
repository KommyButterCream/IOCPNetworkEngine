#include "SendPacketPool.h"

#include <malloc.h> // for _aligned_malloc, _aligned_free
#include <new> // for placement-new

#include "../Diagnostics/EngineAssert.h"

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
	::InitializeSListHead(&m_sListHead);
}

SendPacketBuffer* SendPacketPool::Acquire()
{
	if (!m_bufferPool)
	{
		return nullptr;
	}

	PSLIST_ENTRY entry = ::InterlockedPopEntrySList(&m_sListHead);

	if (!entry)
	{
		// 풀 고갈. 이 풀은 세션이 공유하므로(HybridSendPacketPool 샤딩)
		// 고갈은 곧 해당 샤드의 송신 실패를 뜻한다.
		LOGW("send packet pool exhausted (block count %u). sends on this shard will fail", m_blockCount);
		return nullptr;
	}

	SendPacketBuffer* packetBuffer = static_cast<SendPacketBuffer*>(entry);

	// 프리 리스트에서 꺼낸 블록은 반드시 in-use 가 아니어야 한다.
	// 어긋나면 리스트가 이미 오염된 것이므로 임대하지 않는다.
	if (::InterlockedCompareExchange(&packetBuffer->inUse, 1, 0) != 0)
	{
		ENGINE_VIOLATION("send packet pool free list is corrupted : block %p was already in use",
			static_cast<const void*>(packetBuffer));
		return nullptr;
	}

	packetBuffer->Reset();

	return packetBuffer;
}

void SendPacketPool::Release(SendPacketBuffer* packetBuffer)
{
	if (!m_bufferPool || !packetBuffer)
		return;

	// 반납은 블록당 정확히 한 번이어야 한다.
	//
	// 이 전이를 이긴 쪽만 리스트에 밀어 넣는다. 가드가 없으면 같은 블록이
	// 두 번 들어가 SList 에 자기참조 순환이 생기고, 그 뒤 Acquire 가 사용
	// 중인 블록을 반복해서 배포한다. 그때는 이미 두 세션이 같은 송신
	// 서술자를 공유하는 상태라 원인 추적이 불가능하다.
	if (::InterlockedCompareExchange(&packetBuffer->inUse, 0, 1) != 1)
	{
		ENGINE_VIOLATION("send packet buffer %p released twice (or never acquired)",
			static_cast<const void*>(packetBuffer));
		return;
	}

	// 들고 있던 패킷 주소를 남겨 두면, 반납된 블록이 해제된 메모리를
	// 가리킨 채 풀에 누워 있게 된다. 임대 시에도 Reset 하지만 여기서
	// 지워야 그 사이의 상태가 정직하다.
	packetBuffer->Reset();

	::InterlockedPushEntrySList(&m_sListHead, static_cast<PSLIST_ENTRY>(packetBuffer));
}
