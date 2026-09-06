#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>
#include <stdint.h>

#include "GlobalBlockPool.h"
#include "SizeBin.h"
#include "ThreadBlockCache.h"

#ifndef IOCP_ENGINE_API
#ifdef BUILD_IOCP_ENGINE_DLL
#define IOCP_ENGINE_API __declspec(dllexport)
#else
#define IOCP_ENGINE_API __declspec(dllimport)
#endif
#endif

// TLS 2단계 크기별 메모리 풀 — 조립과 공개 API.
//
// 실제 일은 세 부분이 나눠서 한다.
//
//   BinTable          SizeBin.h           설정 -> 빈 상수. 상태 없음
//   GlobalBlockPool   GlobalBlockPool.h   세그먼트 소유 + 묶음 보관
//   ThreadBlockCache  ThreadBlockCache.h  스레드별 프리 리스트 + 수명
//
// 요청 하나가 지나는 길
//
//   Acquire(size)
//     +-- BinTable::BinOf(size)            BitScanReverse 1회, 메모리 접근 0
//     +-- 최대 빈 초과 -> HeapAlloc 우회
//     +-- ThreadCacheRegistry::GetOrCreate()
//     +-- ThreadBlockCache::Pop(bin)       락 0. 대부분 여기서 끝난다
//           +-- 비었으면 Refill -> GlobalBlockPool::PopChunk()   Interlocked
//                 +-- 재고 없으면 세그먼트 확장                   SRWLOCK
//
// 종료 계약
//   Finalize 는 이 풀을 사용하던 모든 스레드가 종료되었거나 최소한 더 이상
//   할당하지 않는 시점에 호출해야 한다. 엔진은 워커 스레드를 join 한 뒤
//   풀을 지우므로 이 계약을 이미 만족한다.

class IOCP_ENGINE_API TlsMemoryPool
{
public:
	// 메모리 풀 초기화에 사용.
	//
	// blockSize 는 "정확한 블록 크기" 가 아니라 "이 크기를 담을 수 있는
	// 2의 거듭제곱 빈을 만들어라" 는 뜻이다. 빈 개수는 항목 수가 아니라
	// 최소~최대 범위가 정한다.
	// blockCount 는 그 빈에 미리 만들어 둘 블록 수다. 세그먼트 최소 크기
	// 때문에 요청보다 많이 만들어질 수 있다.
	struct BinConfig
	{
		uint32_t blockSize = 0;
		uint32_t blockCount = 0;
	};

	// 빈 하나의 운영 지표.
	//
	//   blockCount     : 세그먼트에서 실제로 만들어낸 블록 수 (정확값)
	//   allocatedCount : 지금 사용 중인 블록 수 (acquire - release, 정확값)
	//   peakAllocated  : 전역 재고 최저점 기준. 동시 사용량의 상한이다
	//   growthCount    : 런타임 확장 횟수. 0 이 아니면 초기 blockCount 가 부족했다
	struct BinStats
	{
		uint32_t blockSize = 0;
		uint32_t blockCount = 0;
		uint32_t allocatedCount = 0;
		uint32_t peakAllocated = 0;
		uint32_t growthCount = 0;
		uint32_t acquireFailCount = 0;
		uint64_t totalAcquire = 0;
		uint64_t totalRelease = 0;

		uint32_t blocksInPool = 0;    // 전역 SLIST 에 있는 수
		uint32_t blocksInCache = 0;   // 살아있는 스레드 캐시들이 들고 있는 수
		uint32_t cacheCapBlocks = 0;
		uint32_t batchBlocks = 0;

		uint32_t segmentCount = 0;
		uint64_t committedBytes = 0;   // 이 빈이 OS 에서 잡은 총 바이트
	};

	// 예전 이름. 호출부 호환용으로 남긴다.
	using SlabConfig = BinConfig;
	using SlabStats = BinStats;

public:
	TlsMemoryPool() = default;
	~TlsMemoryPool();

	TlsMemoryPool(const TlsMemoryPool&) = delete;
	TlsMemoryPool& operator=(const TlsMemoryPool&) = delete;

public:
	// payloadAlignment 는 2의 거듭제곱이어야 하고 16 이상이어야 한다.
	// Job 처럼 alignas(64) 인 타입을 담는 풀은 64 를 줘야 한다.
	bool Initialize(const BinConfig* configs, uint32_t configCount, uint32_t payloadAlignment = 16);
	void Finalize();

	void* Acquire(size_t size);
	void  Release(const void* payload);

	uint32_t GetBinCount() const { return m_table.binCount; }
	bool GetBinStats(uint32_t bin, BinStats& outStats) const;
	void LogStats(const char* poolName) const;

	// 이 풀이 다루는 빈의 블록 크기. 진단 / 샘플 코드용.
	uint32_t GetBinBlockSize(uint32_t bin) const;

	// size 가 어느 빈으로 가는지. binCount 이상이면 OS 우회다. 진단용.
	uint32_t GetBinIndex(size_t size) const;

	// 예전 이름. 호출부 호환용.
	uint32_t GetSlabCount() const { return GetBinCount(); }
	bool GetSlabStats(uint32_t bin, BinStats& out) const { return GetBinStats(bin, out); }
	uint32_t GetClassBlockSize(uint32_t bin) const { return GetBinBlockSize(bin); }
	uint32_t GetClassIndex(size_t size) const { return GetBinIndex(size); }

private:
	void* AcquireBypass(size_t size);
	void  ReleaseBypass(MemoryPoolDetail::BlockHeader* header);

private:
	MemoryPoolDetail::BinTable            m_table;
	MemoryPoolDetail::GlobalBlockPool     m_global;
	MemoryPoolDetail::ThreadCacheRegistry m_registry;

	bool m_initialized = false;

	// OS 우회 경로 지표
	mutable volatile LONG64 m_bypassAcquire = 0;
	mutable volatile LONG64 m_bypassRelease = 0;
};
