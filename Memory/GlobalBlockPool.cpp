#include "GlobalBlockPool.h"

#include <string.h>

#include "../Diagnostics/EngineAssert.h"

#include "../../Core/Util/Logger.h"

using namespace Core::Util;

namespace MemoryPoolDetail
{
	// 전역 리스트에 오르내리는 묶음의 머리.
	//
	// 별도 할당을 하지 않고 묶음 첫 블록의 "페이로드" 자리를 빌려 쓴다.
	// 그 블록은 전역에 있는 동안 아무도 페이로드를 쓰지 않으므로 안전하고,
	// 페이로드는 이미 16바이트 이상 정렬이라 SLIST_ENTRY 요건도 만족한다.
	// 그래서 묶음 관리에 드는 추가 메모리가 0바이트다.
	//
	// SLIST_ENTRY 는 반드시 첫 필드여야 한다. Pop 이 돌려준 entry 주소를
	// 그대로 ChunkNode* 로 캐스팅하기 때문이다.
	struct ChunkNode
	{
		SLIST_ENTRY entry;
		BlockHeader* blockHead;
		uint32_t blockCount;
		uint32_t reserved;
	};

	static_assert(sizeof(ChunkNode) <= MIN_BLOCK_SIZE,
		"chunk node must fit inside the smallest block payload");

	// 빈 하나의 전역 상태.
	// 캐시 라인을 빈마다 분리해서 false sharing 을 막는다.
	struct GlobalBlockPool::Bin
	{
		SLIST_HEADER chunkList;          // 16. 묶음들의 락프리 스택
		SRWLOCK growLock;                // 8.  세그먼트 확장에서만 잡힌다
		volatile LONG64 blocksInPool;    // 8
		volatile LONG64 blocksCreated;   // 8
		volatile LONG growthCount;       // 4
		volatile LONG failCount;         // 4
		void** segments;                 // 8
		volatile LONG64 committedBytes;  // 8. 이 빈이 OS 에서 잡은 총 바이트
		uint32_t segmentCount;           // 4
		uint32_t segmentCapacity;        // 4

		// 전역 재고의 최저점. blocksCreated 에서 이 값을 빼면 동시에 밖에
		// 나가 있던 블록 수의 상한이 된다. 풀 크기를 정하는 근거다.
		// 스레드별 카운터로는 구할 수 없다. 생산 스레드와 소비 스레드가
		// 다르면 생산 쪽 카운터는 단조 증가만 하기 때문이다.
		volatile LONG64 minBlocksInPool; // 8

		char pad[48];
	};

	// ---------------------------------------------------------------------

	GlobalBlockPool::~GlobalBlockPool()
	{
		Finalize();
	}

	bool GlobalBlockPool::Initialize(const BinTable& table, uint8_t ownerTag)
	{
		// 중첩 타입이 private 이라 파일 범위에서는 검사할 수 없어 여기에 둔다.
		static_assert(sizeof(Bin) == 128, "global bin must span exactly two cache lines");

		if (m_initialized)
			return false;

		m_table = &table;
		m_ownerTag = ownerTag;

		// SLIST_HEADER 와 false sharing 회피를 위해 64바이트 정렬이 필요한데
		// HeapAlloc 은 16바이트까지만 보장하므로 직접 맞춘다.
		HANDLE heap = ::GetProcessHeap();
		const size_t bytes = sizeof(Bin) * table.binCount + 64;

		m_binsRaw = ::HeapAlloc(heap, HEAP_ZERO_MEMORY, bytes);
		if (!m_binsRaw)
		{
			LOGE("failed to allocate %zu bytes for %u global bins", bytes, table.binCount);
			return false;
		}

		m_bins = reinterpret_cast<Bin*>(
			AlignUpSize(reinterpret_cast<uintptr_t>(m_binsRaw), 64));

		for (uint32_t b = 0; b < table.binCount; ++b)
		{
			::InitializeSListHead(&m_bins[b].chunkList);
			::InitializeSRWLock(&m_bins[b].growLock);

			// 아직 한 번도 꺼내가지 않았다는 뜻. 첫 Pop 에서 실제 값으로 내려간다.
			m_bins[b].minBlocksInPool = MAXLONGLONG;
		}

		m_initialized = true;
		return true;
	}

	void GlobalBlockPool::Finalize()
	{
		if (!m_bins)
		{
			m_initialized = false;
			return;
		}

		HANDLE heap = ::GetProcessHeap();
		const uint32_t binCount = m_table ? m_table->binCount : 0;

		for (uint32_t b = 0; b < binCount; ++b)
		{
			Bin& target = m_bins[b];

			for (uint32_t s = 0; s < target.segmentCount; ++s)
			{
				if (target.segments[s])
					::VirtualFree(target.segments[s], 0, MEM_RELEASE);
			}

			if (target.segments)
				::HeapFree(heap, 0, target.segments);

			target.segments = nullptr;
			target.segmentCount = 0;
			target.segmentCapacity = 0;
			target.committedBytes = 0;
		}

		if (m_binsRaw)
		{
			::HeapFree(heap, 0, m_binsRaw);
			m_binsRaw = nullptr;
		}

		m_bins = nullptr;
		m_table = nullptr;
		m_initialized = false;
	}

	bool GlobalBlockPool::Preallocate(uint32_t bin, uint32_t blocks)
	{
		if (!m_initialized || bin >= m_table->binCount || blocks == 0)
			return false;

		Bin& target = m_bins[bin];

		::AcquireSRWLockExclusive(&target.growLock);
		const bool ok = GrowLocked(bin, blocks, true);
		::ReleaseSRWLockExclusive(&target.growLock);

		return ok;
	}

	GlobalBlockPool::ChunkRef GlobalBlockPool::PopChunk(uint32_t bin)
	{
		Bin& target = m_bins[bin];

		PSLIST_ENTRY entry = ::InterlockedPopEntrySList(&target.chunkList);

		if (!entry)
		{
			// 재고가 비었다. 이 함수에서만 락을 잡는다.
			::AcquireSRWLockExclusive(&target.growLock);

			// 락을 기다리는 동안 다른 스레드가 채웠을 수 있다.
			// 이 재시도가 없으면 스레드 수만큼 세그먼트가 중복으로 잡힌다.
			entry = ::InterlockedPopEntrySList(&target.chunkList);
			if (!entry)
			{
				if (GrowLocked(bin, m_table->Spec(bin).batchBlocks, false))
					entry = ::InterlockedPopEntrySList(&target.chunkList);
			}

			::ReleaseSRWLockExclusive(&target.growLock);
		}

		ChunkRef ref;
		if (!entry)
		{
			::InterlockedIncrement(&target.failCount);
			return ref;
		}

		ChunkNode* node = reinterpret_cast<ChunkNode*>(entry);
		ref.head = node->blockHead;
		ref.count = node->blockCount;

		NotePopped(target, ref.count);
		return ref;
	}

	void GlobalBlockPool::PushChunk(uint32_t bin, BlockHeader* head, uint32_t count)
	{
		if (!head || count == 0)
			return;

		Bin& target = m_bins[bin];

		// 묶음 머리 블록의 페이로드에 꼬리표를 써넣는다.
		ChunkNode* node = static_cast<ChunkNode*>(PayloadOf(head, m_table->headerSize));
		node->blockHead = head;
		node->blockCount = count;

		::InterlockedPushEntrySList(&target.chunkList, &node->entry);
		::InterlockedExchangeAdd64(&target.blocksInPool, static_cast<LONG64>(count));
	}

	void GlobalBlockPool::NotePopped(Bin& target, uint32_t count)
	{
		const LONG64 remaining =
			::InterlockedExchangeAdd64(&target.blocksInPool, -static_cast<LONG64>(count))
			- static_cast<LONG64>(count);

		// 재고 최저점을 갱신한다. 묶음마다 한 번만 도는 경로라 CAS 루프가
		// 부담이 없고, 이 값이 풀 크기를 정하는 유일한 근거다.
		for (;;)
		{
			const LONG64 current = ::InterlockedCompareExchange64(&target.minBlocksInPool, 0, 0);
			if (remaining >= current)
				break;
			if (::InterlockedCompareExchange64(&target.minBlocksInPool, remaining, current) == current)
				break;
		}
	}

	bool GlobalBlockPool::RecordSegmentLocked(Bin& target, void* segment, size_t bytes)
	{
		HANDLE heap = ::GetProcessHeap();

		if (target.segmentCount >= target.segmentCapacity)
		{
			// 4 -> 8 -> 16 ... 배로 키운다. +1 씩 늘리면 추가가 O(N^2) 가 된다.
			// 설정이 맞으면 대개 첫 확장(0 -> 4) 한 번으로 끝난다.
			const uint32_t newCapacity = (target.segmentCapacity == 0) ? 4 : target.segmentCapacity * 2;
			const size_t   newBytes = sizeof(void*) * newCapacity;

			// HeapReAlloc 은 뒤에 여유가 있으면 제자리에서 늘려주고, 실패해도
			// 기존 블록을 그대로 남긴다. 손으로 alloc / memcpy / free 할 이유가 없다.
			// (HEAP_ZERO_MEMORY 는 늘어난 뒷부분만 0 으로 채운다)
			void** grown = target.segments
				? static_cast<void**>(::HeapReAlloc(heap, HEAP_ZERO_MEMORY, target.segments, newBytes))
				: static_cast<void**>(::HeapAlloc(heap, HEAP_ZERO_MEMORY, newBytes));

			if (!grown)
			{
				LOGE("failed to grow the segment list to %u entries", newCapacity);
				return false;
			}

			target.segments = grown;
			target.segmentCapacity = newCapacity;
		}

		target.segments[target.segmentCount] = segment;
		++target.segmentCount;

		// growLock 을 잡은 채로만 부르지만, 읽는 쪽(GetStats)은 락 밖이라
		// 다른 카운터들과 같은 방식으로 맞춘다.
		::InterlockedExchangeAdd64(&target.committedBytes, static_cast<LONG64>(bytes));

		return true;
	}

	void GlobalBlockPool::CarveAndPush(Bin& target, uint32_t bin, void* segment, uint32_t blocks)
	{
		const BinSpec& spec = m_table->Spec(bin);
		const uint32_t headerSize = m_table->headerSize;

		char* cursor = static_cast<char*>(segment);
		uint32_t remaining = blocks;

		while (remaining > 0)
		{
			uint32_t take = spec.batchBlocks;
			if (take > remaining)
				take = remaining;      // 마지막 묶음은 batch 보다 작을 수 있다

			BlockHeader* head = nullptr;
			for (uint32_t i = 0; i < take; ++i)
			{
				BlockHeader* header =
					reinterpret_cast<BlockHeader*>(cursor + static_cast<size_t>(spec.stride) * i);

				header->magic = MAGIC_FREE;
				header->binIndex = static_cast<uint8_t>(bin);
				header->poolId = m_ownerTag;
				header->reserved = 0;
				header->next = head;
				head = header;
			}

			cursor += static_cast<size_t>(spec.stride) * take;
			remaining -= take;

			ChunkNode* node = static_cast<ChunkNode*>(PayloadOf(head, headerSize));
			node->blockHead = head;
			node->blockCount = take;

			::InterlockedPushEntrySList(&target.chunkList, &node->entry);
			::InterlockedExchangeAdd64(&target.blocksInPool, static_cast<LONG64>(take));
		}
	}

	bool GlobalBlockPool::GrowLocked(uint32_t bin, uint32_t minBlocks, bool isInitial)
	{
		Bin& target = m_bins[bin];
		const BinSpec& spec = m_table->Spec(bin);

		uint32_t blocks = spec.blocksPerSegment;
		if (blocks < minBlocks)
			blocks = minBlocks;

		const size_t bytes = static_cast<size_t>(spec.stride) * blocks;

		// VirtualAlloc 은 64KB 경계로 정렬된 주소를 준다.
		// stride 와 headerSize 가 payloadAlignment 의 배수이므로
		// 모든 블록의 페이로드가 payloadAlignment 로 정렬된다.
		void* segment = ::VirtualAlloc(nullptr, bytes, MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE);
		if (!segment)
		{
			LOGE("bin %u (block size %u) failed to reserve a %zu byte segment (error %lu)",
				bin, spec.blockSize, bytes, ::GetLastError());
			::InterlockedIncrement(&target.failCount);
			return false;
		}

		if (!RecordSegmentLocked(target, segment, bytes))
		{
			::VirtualFree(segment, 0, MEM_RELEASE);
			::InterlockedIncrement(&target.failCount);
			return false;
		}

		CarveAndPush(target, bin, segment, blocks);
		::InterlockedExchangeAdd64(&target.blocksCreated, static_cast<LONG64>(blocks));

		if (!isInitial)
		{
			// 초기 blockCount 가 부족했다는 신호다. 튜닝 근거로 남긴다.
			::InterlockedIncrement(&target.growthCount);
			LOGW("bin %u (block size %u) grew at runtime : +%u blocks, %lld total",
				bin, spec.blockSize, blocks,
				::InterlockedCompareExchange64(&target.blocksCreated, 0, 0));
		}

		return true;
	}

	bool GlobalBlockPool::GetStats(uint32_t bin, BinStats& out) const
	{
		if (!m_initialized || bin >= m_table->binCount)
			return false;

		Bin& target = m_bins[bin];

		out = BinStats{};
		out.blocksCreated = static_cast<uint32_t>(
			::InterlockedCompareExchange64(&target.blocksCreated, 0, 0));
		out.blocksInPool = static_cast<uint32_t>(
			::InterlockedCompareExchange64(&target.blocksInPool, 0, 0));
		out.growthCount = static_cast<uint32_t>(
			::InterlockedCompareExchange(&target.growthCount, 0, 0));
		out.failCount = static_cast<uint32_t>(
			::InterlockedCompareExchange(&target.failCount, 0, 0));
		out.segmentCount = target.segmentCount;
		out.committedBytes = static_cast<uint64_t>(
			::InterlockedCompareExchange64(&target.committedBytes, 0, 0));

		// 재고가 가장 적었던 순간에 밖에 나가 있던 블록 수.
		// 그중 일부는 스레드 캐시에 놀고 있었을 수 있으므로 상한이다.
		const LONG64 minInPool = ::InterlockedCompareExchange64(&target.minBlocksInPool, 0, 0);
		out.peakOutOfPool = (minInPool >= 0 && minInPool <= static_cast<LONG64>(out.blocksCreated))
			? static_cast<uint32_t>(static_cast<LONG64>(out.blocksCreated) - minInPool)
			: 0;

		return true;
	}
}
