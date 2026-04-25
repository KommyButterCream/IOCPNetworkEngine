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

	// 1. Slab ����ü �迭 �Ҵ�
	m_slabs = static_cast<Slab*>(::HeapAlloc(::GetProcessHeap(), HEAP_ZERO_MEMORY, sizeof(Slab) * slabCount));
	if (!m_slabs) return false;

	m_slabCount = slabCount;

	for (uint32_t i = 0; i < slabCount; ++i)
	{
		// placement new�� SRWLOCK �� �ʱ�ȭ
		new (&m_slabs[i]) Slab();

		m_slabs[i].blockSize = configs[i].blockSize;
		m_slabs[i].blockCount = configs[i].blockCount;

		// ����� ���̷ε� ���� ���
		size_t headerSize = AlignUp(sizeof(BlockHeader), MEMORY_ALIGNMENT);
		size_t payloadSize = AlignUp(configs[i].blockSize, MEMORY_ALIGNMENT);
		m_slabs[i].stride = static_cast<uint32_t>(headerSize + payloadSize);

		size_t totalBytes = static_cast<size_t>(m_slabs[i].stride) * configs[i].blockCount;
		void* memory = ::HeapAlloc(::GetProcessHeap(), HEAP_ZERO_MEMORY, totalBytes);

		if (!memory) return false;

		m_slabs[i].initialMemory = memory;

		// ���� ����Ʈ ����
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

	// ��� �޸𸮰� ��ȯ�Ǿ����� Ȯ�� (����׿�)
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
		// ����� �Ҹ��� ȣ�� (SRWLOCK ���� �ý��� �ڿ��� �ƴϹǷ� ���� �����ϳ� ��Ģ�� ȣ��)
		m_slabs[i].~Slab();
	}
	::HeapFree(::GetProcessHeap(), 0, m_slabs);
	m_slabs = nullptr;
	m_slabCount = 0;
	m_initialized = false;
}

uint32_t SlabMemoryPool::FindSlabIndex(size_t memorySize) const
{
	// Slab �� BlockSize �� �׻� ���� �Ǿ� �ִٰ� �����Ѵ�.
	// ó������ blockSize�� memorySize �̻��� slab �ε��� ��ȯ
	// ������ UINT32_MAX ��ȯ

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

	// ���� �迭 Ȯ��
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
			// ���� ��ȯ �� �� ����� ����
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
		// ��Ÿ�� �Ҵ� �߻� (�� ������ "�츮��" ���� ����)
		// ���� � ȯ�濡���� �ʱ� nBlockCount�� ���� ũ�� ��� ���� �����ϴ�.
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

	// ���� �ѹ� üũ (��� �ļ� ���� Ȯ��)
	if (header->magic != HEADER_MAGIC) {
		__debugbreak(); // �޸� ���� �߻�!
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
