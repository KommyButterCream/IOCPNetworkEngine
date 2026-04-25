#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>
#include <stdint.h>

#ifdef BUILD_IOCP_ENGINE_DLL
#define IOCP_ENGINE_API __declspec(dllexport)
#else
#define IOCP_ENGINE_API __declspec(dllimport)
#endif

class IOCP_ENGINE_API SlabMemoryPool
{
	// Define Structure
public:
	// Slab �޸� ������ ���� ����ü
	// �޸� Ǯ �ʱ�ȭ�� ���
	struct SlabConfig
	{
		uint32_t blockSize = 0;
		uint32_t blockCount = 0;
	};

	// ���� �ѹ�: �޸� ���� �� ��� ��ġ ������
	static constexpr uint32_t HEADER_MAGIC = 0xDEADBEEF;

public:
	SlabMemoryPool();
	~SlabMemoryPool();

public:
	bool Initialize(const SlabConfig* configs, uint32_t slabCount);
	void Finalize();

	void* Acquire(size_t size);
	void Release(const void* payload);

private:
	// Slab �޸� Ǯ�� ��ũ�� ����Ʈ
	struct alignas(16) BlockHeader
	{
		uint32_t bucketIndex = UINT32_MAX;
		uint32_t magic = HEADER_MAGIC;
		BlockHeader* next = nullptr;
	};

	// False Sharing �� �����ϱ� ���� Padding �� ĳ�� ���� 64����Ʈ �����
	static constexpr size_t CACHE_LINE_SIZE = 64;
	static constexpr size_t MEMORY_ALIGNMENT = 16; // IOCP ���� ����
	struct alignas(CACHE_LINE_SIZE) Slab
	{
		SRWLOCK lock = SRWLOCK_INIT;               // ���� �ֻ�ܿ� ��ġ
		BlockHeader* freeList = nullptr;

		uint32_t allocatedCount = 0;
		uint32_t blockSize = 0;
		uint32_t stride = 0;
		uint32_t blockCount = 0;

		void* initialMemory = nullptr;       // �ʱ� �Ҵ� ����
		void** extraMemoryBlocks = nullptr;
		uint32_t extraMemoryCount = 0;
		uint32_t extraMemoryCapacity = 0;

		Slab()
		{
		}
	};

	Slab* m_slabs = nullptr;
	uint32_t m_slabCount = 0;
	bool m_initialized = false;

	uint32_t FindSlabIndex(size_t memorySize) const;

	// �� �߰� �޸� ��� �Ҵ� �� ���� ����Ʈ�� ����
	bool AllocateExtraBlocks(Slab& slab, uint32_t blockCount);

	// Align helpers
	static size_t AlignUp(size_t memorySize, size_t alignment)
	{
		// �Է¹��� �޸� ũ�� memorySize �̻��� �����鼭
		// alignment �� ����� �����ϴ� ũ��� ��ȯ�Ѵ�.
		return (memorySize + (alignment - 1)) & ~(alignment - 1);
	}

	bool VerifyAllMemoryReleased() const;
};
