#include "SlabMemoryPool.h"

#include <stdio.h>

#include <new>

SlabMemoryPool::SlabMemoryPool()
{
}

SlabMemoryPool::~SlabMemoryPool()
{
	Finalize();
}

bool SlabMemoryPool::Initialize(const SlabConfig* configs, uint32_t slabCount)
{
	if (!configs || slabCount == 0 || m_initialized) return false;

	// 1. Slab 구조체 배열 할당
	m_slabs = static_cast<Slab*>(::HeapAlloc(::GetProcessHeap(), HEAP_ZERO_MEMORY, sizeof(Slab) * slabCount));
	if (!m_slabs) return false;

	m_slabCount = slabCount;

	for (uint32_t i = 0; i < slabCount; ++i)
	{
		// placement new 로 SRWLOCK 을 초기화
		new (&m_slabs[i]) Slab();

		m_slabs[i].blockSize = configs[i].blockSize;
		m_slabs[i].blockCount = configs[i].blockCount;

		// 헤더와 페이로드 영역 크기 계산
		size_t headerSize = AlignUp(sizeof(BlockHeader), MEMORY_ALIGNMENT);
		size_t payloadSize = AlignUp(configs[i].blockSize, MEMORY_ALIGNMENT);
		m_slabs[i].stride = static_cast<uint32_t>(headerSize + payloadSize);

		size_t totalBytes = static_cast<size_t>(m_slabs[i].stride) * configs[i].blockCount;
		void* memory = ::HeapAlloc(::GetProcessHeap(), HEAP_ZERO_MEMORY, totalBytes);

		if (!memory) return false;

		m_slabs[i].initialMemory = memory;

		// 프리 리스트 구성
		char* cursor = static_cast<char*>(memory);
		for (uint32_t blockIndex = 0; blockIndex < configs[i].blockCount; ++blockIndex)
		{
			BlockHeader* header = reinterpret_cast<BlockHeader*>(cursor);
			header->bucketIndex = i;
			header->magic = HEADER_MAGIC;
			header->next = m_slabs[i].freeList;
			m_slabs[i].freeList = header;
			cursor += m_slabs[i].stride;
		}
	}

	m_initialized = true;
	return true;
}
void SlabMemoryPool::Finalize()
{
	if (!m_initialized) return;

	// 모든 메모리가 반환되었는지 확인 (디버그용)
	bool allMemoryReleased = VerifyAllMemoryReleased();
	if (!allMemoryReleased)
	{
		__debugbreak();
	}

	for (uint32_t i = 0; i < m_slabCount; ++i)
	{
		if (m_slabs[i].initialMemory)
			::HeapFree(::GetProcessHeap(), 0, m_slabs[i].initialMemory);

		if (m_slabs[i].extraMemoryBlocks)
		{
			for (uint32_t j = 0; j < m_slabs[i].extraMemoryCount; ++j)
			{
				if (m_slabs[i].extraMemoryBlocks[j])
					::HeapFree(::GetProcessHeap(), 0, m_slabs[i].extraMemoryBlocks[j]);
			}
			::HeapFree(::GetProcessHeap(), 0, m_slabs[i].extraMemoryBlocks);
		}
		// 명시적 소멸자 호출 (SRWLOCK 은 해제가 필요한 시스템 자원은 아니지만 원칙대로 호출)
		m_slabs[i].~Slab();
	}
	::HeapFree(::GetProcessHeap(), 0, m_slabs);
	m_slabs = nullptr;
	m_slabCount = 0;
	m_initialized = false;
}

uint32_t SlabMemoryPool::FindSlabIndex(size_t memorySize) const
{
	// Slab 의 blockSize 는 항상 오름차순으로 정렬되어 있다고 가정한다.
	// 처음으로 blockSize 가 memorySize 이상이 되는 slab 인덱스를 반환하고
	// 없으면 UINT32_MAX 를 반환한다.

	for (uint32_t slabId = 0; slabId < m_slabCount; ++slabId)
	{
		if (m_slabs[slabId].blockSize >= memorySize)
		{
			return slabId;
		}
	}

	return UINT32_MAX;
}

bool SlabMemoryPool::AllocateExtraBlocks(Slab& slab, uint32_t blockCount)
{
	size_t allocSize = static_cast<size_t>(slab.stride) * blockCount;
	void* memory = ::HeapAlloc(::GetProcessHeap(), HEAP_ZERO_MEMORY, allocSize);
	if (!memory) return false;

	// 관리 배열 확장
	if (slab.extraMemoryCount >= slab.extraMemoryCapacity)
	{
		uint32_t newCapacity = (slab.extraMemoryCapacity == 0) ? 4 : slab.extraMemoryCapacity * 2;
		void** newArray = static_cast<void**>(::HeapAlloc(::GetProcessHeap(), HEAP_ZERO_MEMORY, sizeof(void*) * newCapacity));
		if (!newArray) {
			::HeapFree(::GetProcessHeap(), 0, memory);
			return false;
		}

		if (slab.extraMemoryBlocks)
		{
			memcpy(newArray, slab.extraMemoryBlocks, sizeof(void*) * slab.extraMemoryCount);
			::HeapFree(::GetProcessHeap(), 0, slab.extraMemoryBlocks);
		}
		slab.extraMemoryBlocks = newArray;
		slab.extraMemoryCapacity = newCapacity;
	}

	slab.extraMemoryBlocks[slab.extraMemoryCount++] = memory;

	uint32_t slabIndex = static_cast<uint32_t>(&slab - m_slabs);
	char* cursor = static_cast<char*>(memory);
	for (uint32_t i = 0; i < blockCount; ++i)
	{
		BlockHeader* header = reinterpret_cast<BlockHeader*>(cursor);
		header->bucketIndex = slabIndex;
		header->magic = HEADER_MAGIC;
		header->next = slab.freeList;
		slab.freeList = header;
		cursor += slab.stride;
	}
	return true;
}

bool SlabMemoryPool::VerifyAllMemoryReleased() const
{
	for (uint32_t i = 0; i < m_slabCount; ++i)
	{
		::AcquireSRWLockShared(&m_slabs[i].lock);
		if (m_slabs[i].allocatedCount != 0)
		{
			// 아직 반환되지 않은 블록이 남아 있음
			::ReleaseSRWLockShared(&m_slabs[i].lock);
			return false;
		}
		::ReleaseSRWLockShared(&m_slabs[i].lock);
	}
	return true;
}

void* SlabMemoryPool::Acquire(size_t size)
{
	if (!m_initialized || size == 0) return nullptr;

	uint32_t slabIndex = FindSlabIndex(size);
	if (slabIndex == UINT32_MAX) return nullptr;

	Slab& slab = m_slabs[slabIndex];
	BlockHeader* header = nullptr;

	::AcquireSRWLockExclusive(&slab.lock);

	if (!slab.freeList)
	{
		// 프리 리스트 고갈로 런타임 추가 할당 발생 (느린 경로)
		// 실제 운영 환경에서는 초기 blockCount 를 충분히 크게 잡아 이 경로를 피하는 것이 좋다.
		if (!AllocateExtraBlocks(slab, slab.blockCount))
		{
			::ReleaseSRWLockExclusive(&slab.lock);
			return nullptr;
		}
	}

	header = slab.freeList;
	if (header)
	{
		slab.freeList = header->next;
		slab.allocatedCount++;
	}

	::ReleaseSRWLockExclusive(&slab.lock);

	if (!header) return nullptr;

	size_t headerSize = AlignUp(sizeof(BlockHeader), MEMORY_ALIGNMENT);
	return reinterpret_cast<char*>(header) + headerSize;
}

void SlabMemoryPool::Release(const void* payload)
{
	// Release block previously acquired. Caller MUST pass pointer returned by Acquire.

	if (!payload) return;

	size_t headerSize = AlignUp(sizeof(BlockHeader), MEMORY_ALIGNMENT);
	BlockHeader* header = reinterpret_cast<BlockHeader*>(const_cast<char*>(reinterpret_cast<const char*>(payload) - headerSize));

	// 매직 넘버 체크 (헤더 훼손 여부 확인)
	if (header->magic != HEADER_MAGIC) {
		__debugbreak(); // 메모리 훼손 발생!
		return;
	}

	uint32_t slabIndex = header->bucketIndex;
	Slab& slab = m_slabs[slabIndex];

	::AcquireSRWLockExclusive(&slab.lock);
	header->next = slab.freeList;
	slab.freeList = header;
	slab.allocatedCount--;
	::ReleaseSRWLockExclusive(&slab.lock);
}
