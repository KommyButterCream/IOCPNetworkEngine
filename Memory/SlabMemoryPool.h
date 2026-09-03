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
	// Slab 메모리 설정을 위한 구조체
	// 메모리 풀 초기화에 사용
	struct SlabConfig
	{
		uint32_t blockSize = 0;
		uint32_t blockCount = 0;
	};

	// 매직 넘버: 메모리 해제 시 헤더 훼손 여부 검증용
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
	// Slab 메모리 풀의 링크드 리스트 노드
	struct alignas(16) BlockHeader
	{
		uint32_t bucketIndex = UINT32_MAX;
		uint32_t magic = HEADER_MAGIC;
		BlockHeader* next = nullptr;
	};

	// False Sharing 을 방지하기 위해 Padding 을 넣고 캐시 라인 64바이트로 정렬
	static constexpr size_t CACHE_LINE_SIZE = 64;
	static constexpr size_t MEMORY_ALIGNMENT = 16; // IOCP 요구 정렬
	struct alignas(CACHE_LINE_SIZE) Slab
	{
		SRWLOCK lock = SRWLOCK_INIT;               // 경합 지점이므로 구조체 최상단에 배치
		BlockHeader* freeList = nullptr;

		uint32_t allocatedCount = 0;
		uint32_t blockSize = 0;
		uint32_t stride = 0;
		uint32_t blockCount = 0;

		void* initialMemory = nullptr;       // 초기 할당 영역
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

	// 새 추가 메모리 블록을 할당한 뒤 프리 리스트에 연결
	bool AllocateExtraBlocks(Slab& slab, uint32_t blockCount);

	// Align helpers
	static size_t AlignUp(size_t memorySize, size_t alignment)
	{
		// 입력받은 메모리 크기 memorySize 이상이면서
		// alignment 의 배수를 만족하는 크기로 반환한다.
		return (memorySize + (alignment - 1)) & ~(alignment - 1);
	}

	bool VerifyAllMemoryReleased() const;
};
