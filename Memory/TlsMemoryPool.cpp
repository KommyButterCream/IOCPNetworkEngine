#include "TlsMemoryPool.h"

#include <intrin.h>
#include <stdio.h>
#include <string.h>

#include "../Diagnostics/EngineAssert.h"

#include "../../Core/Util/Logger.h"

using namespace Core::Util;

namespace
{
	// 스팬 하나의 목표 크기. 클래스 stride 로 나누어 블록 수를 정한다.
	constexpr size_t SPAN_TARGET_BYTES = 256 * 1024;

	// 스레드 캐시가 한 클래스에 담을 수 있는 최대 블록 수.
	// 바이트 상한과 별개로 리스트가 지나치게 길어지는 것을 막는다.
	constexpr uint32_t TLS_CAP_BLOCK_LIMIT = 512;

	// 이 값보다 적게 담기는 클래스는 TLS 를 두지 않고 전역으로 직행한다.
	// 캐시가 2~3개뿐이면 TLS 를 거치는 분기 비용만 늘고 얻는 게 없다.
	constexpr uint32_t TLS_CAP_MIN_BLOCKS = 4;

	inline size_t AlignUpSize(size_t value, size_t alignment)
	{
		return (value + (alignment - 1)) & ~(alignment - 1);
	}

	// x64 가 아닌 구성에서도 컴파일되도록 감싼다.
	inline bool ScanReverse(unsigned long* index, size_t value)
	{
#if defined(_M_X64) || defined(_M_ARM64)
		return ::_BitScanReverse64(index, static_cast<unsigned __int64>(value)) != 0;
#else
		const unsigned long high = static_cast<unsigned long>(value >> 32);
		if (high != 0 && ::_BitScanReverse(index, high))
		{
			*index += 32;
			return true;
		}
		return ::_BitScanReverse(index, static_cast<unsigned long>(value)) != 0;
#endif
	}

	inline bool IsPowerOfTwo(uint32_t value)
	{
		return value != 0 && (value & (value - 1)) == 0;
	}

	// 2의 거듭제곱 올림 후의 지수. CeilLog2(64) == 6, CeilLog2(65) == 7
	inline uint32_t CeilLog2(size_t value)
	{
		if (value <= 1)
			return 0;

		unsigned long index = 0;
		ScanReverse(&index, value - 1);
		return static_cast<uint32_t>(index) + 1u;
	}

	// 풀 슬롯 점유 비트맵. Finalize 가 슬롯을 반납하므로 풀을 만들고 지우기를
	// 반복해도 슬롯이 마르지 않는다.
	volatile LONG64 g_poolSlotMask = 0;

	// 풀 슬롯이 재사용될 때 예전 풀의 스레드 캐시 포인터를 잘못 집는 일이
	// 없도록 세대를 붙인다. 세대가 다르면 포인터를 아예 건드리지 않는다.
	volatile LONG g_generationCounter = 0;

	// 스레드당 슬롯. 핫 경로는 여기서 포인터 하나를 읽는 게 전부다.
	struct TlsSlot
	{
		void* cache = nullptr;
		uint32_t generation = 0;
		uint32_t reserved = 0;
	};

	thread_local TlsSlot t_slots[TlsMemoryPool::MAX_POOL_COUNT];

	uint32_t AcquirePoolSlot()
	{
		for (;;)
		{
			const LONG64 current = ::InterlockedCompareExchange64(&g_poolSlotMask, 0, 0);

			uint32_t slot = TlsMemoryPool::MAX_POOL_COUNT;
			for (uint32_t i = 0; i < TlsMemoryPool::MAX_POOL_COUNT; ++i)
			{
				if ((current & (1LL << i)) == 0)
				{
					slot = i;
					break;
				}
			}

			if (slot == TlsMemoryPool::MAX_POOL_COUNT)
				return UINT32_MAX;

			const LONG64 desired = current | (1LL << slot);
			if (::InterlockedCompareExchange64(&g_poolSlotMask, desired, current) == current)
				return slot;
		}
	}

	void ReleasePoolSlot(uint32_t slot)
	{
		if (slot >= TlsMemoryPool::MAX_POOL_COUNT)
			return;

		for (;;)
		{
			const LONG64 current = ::InterlockedCompareExchange64(&g_poolSlotMask, 0, 0);
			const LONG64 desired = current & ~(1LL << slot);
			if (::InterlockedCompareExchange64(&g_poolSlotMask, desired, current) == current)
				return;
		}
	}
}

// 페이로드 바로 앞에 놓이는 헤더.
// Release 는 이 헤더만 읽어서 크기 인자 없이 O(1) 로 원래 클래스를 찾는다.
struct TlsMemoryPool::BlockHeader
{
	uint32_t magic;         // MAGIC_LIVE / MAGIC_FREE. 이중 해제와 훼손을 잡는다
	uint8_t  classIndex;    // CLASS_BYPASS 면 OS 우회 블록
	uint8_t  poolId;        // 다른 풀에 반납하는 실수를 잡는다
	uint16_t reserved;
	BlockHeader* next;      // 프리 리스트 링크. 우회 블록에서는 HeapAlloc 원본 주소
};

// 전역 리스트에 오르내리는 batch 묶음의 머리.
// 별도 할당을 하지 않고 묶음 첫 블록의 "페이로드" 자리를 빌려 쓴다.
// 그 블록은 전역에 있는 동안 아무도 페이로드를 쓰지 않으므로 안전하고,
// 페이로드는 이미 16바이트 이상 정렬이라 SLIST_ENTRY 요건도 만족한다.
struct TlsMemoryPool::ChunkNode
{
	SLIST_ENTRY entry;
	BlockHeader* blockHead;
	uint32_t blockCount;
	uint32_t reserved;
};

struct TlsMemoryPool::Bin
{
	BlockHeader* head;
	uint32_t count;
	uint32_t reserved;
};

struct TlsMemoryPool::ThreadCache
{
	// 핫 데이터를 앞에 몰아둔다. 실제로 쓰는 클래스 수만큼만 L1 에 올라온다.
	Bin bins[MAX_CLASS_COUNT];

	TlsMemoryPool* owner;
	ThreadCache* nextRegistered;
	uint32_t poolId;
	uint32_t classCount;
	DWORD threadId;
	volatile LONG detached;

	// 원자적 연산 없이 갱신한다. 이게 TLS 를 쓰는 이유의 절반이다.
	// 집계는 LogStats / Finalize 가 레지스트리를 훑어서 한다.
	uint64_t acquireCount[MAX_CLASS_COUNT];
	uint64_t releaseCount[MAX_CLASS_COUNT];
};

struct TlsMemoryPool::ClassDesc
{
	uint32_t blockSize;
	uint32_t stride;
	uint32_t tlsCapBlocks;    // 0 이면 TLS 를 두지 않고 전역 직행
	uint32_t batchBlocks;
	uint32_t blocksPerSpan;
	uint32_t reserved;
};

// 클래스별 전역 저장소.
// 캐시 라인을 클래스마다 분리해서 false sharing 을 막는다.
struct TlsMemoryPool::GlobalClass
{
	SLIST_HEADER chunkList;          // 16. batch 묶음들의 락프리 스택
	SRWLOCK growLock;                // 8.  스팬 확장에서만 잡힌다
	volatile LONG64 blocksInGlobal;  // 8
	volatile LONG64 blocksCreated;   // 8
	volatile LONG growthCount;       // 4
	volatile LONG acquireFailCount;  // 4
	void** spans;                    // 8
	size_t* spanBytes;               // 8
	uint32_t spanCount;              // 4
	uint32_t spanCapacity;           // 4
	// 전역 재고의 최저점. blocksCreated 에서 이 값을 빼면 동시에 밖에
	// 나가 있던 블록 수의 상한이 된다. 풀 크기를 정하는 근거다.
	// 스레드별 live 카운터로는 구할 수 없다. 생산 스레드와 소비 스레드가
	// 다르면 생산 쪽 카운터는 단조 증가만 하기 때문이다.
	volatile LONG64 minBlocksInGlobal;  // 8

	char pad[48];
};

TlsMemoryPool::TlsMemoryPool()
{
	// 중첩 타입이 private 이라 파일 범위에서는 검사할 수 없어 여기에 둔다.
	static_assert(sizeof(BlockHeader) == 16, "block header must stay 16 bytes");
	static_assert(sizeof(ChunkNode) <= MIN_BLOCK_SIZE, "chunk node must fit inside the smallest block payload");
	static_assert(sizeof(GlobalClass) == 128, "global class must span exactly two cache lines");
	static_assert(sizeof(Bin) == 16, "bin must stay 16 bytes so the hot array is compact");

	::InitializeSRWLock(&m_registryLock);
}

TlsMemoryPool::~TlsMemoryPool()
{
	Finalize();
}

uint8_t TlsMemoryPool::OwnerTag() const
{
	// 빠른 슬롯을 받은 풀만 블록에 자기 표식을 남긴다.
	// 슬롯이 없는 풀들은 서로 구분할 방법이 없으므로 검사 대상에서 뺀다.
	return (m_poolId < MAX_POOL_COUNT) ? static_cast<uint8_t>(m_poolId) : POOL_ID_NONE;
}

uint32_t TlsMemoryPool::ClassOf(size_t size) const
{
	// m_minMask 를 or 하기 때문에 입력이 0 이어도 BitScanReverse 가 안전하고,
	// 최소 클래스보다 작은 요청은 자연스럽게 클래스 0 으로 접힌다.
	unsigned long index = 0;
	ScanReverse(&index, (size - 1) | m_minMask);
	return static_cast<uint32_t>(index) + 1u - m_minClassLog;
}

void* TlsMemoryPool::PayloadOf(BlockHeader* header) const
{
	return reinterpret_cast<char*>(header) + m_headerSize;
}

TlsMemoryPool::BlockHeader* TlsMemoryPool::HeaderOf(const void* payload) const
{
	return reinterpret_cast<BlockHeader*>(const_cast<char*>(static_cast<const char*>(payload)) - m_headerSize);
}

uint32_t TlsMemoryPool::GetClassBlockSize(uint32_t classIndex) const
{
	if (!m_initialized || classIndex >= m_classCount)
		return 0;

	return m_classes[classIndex].blockSize;
}

uint32_t TlsMemoryPool::GetClassIndex(size_t size) const
{
	if (!m_initialized || size == 0)
		return UINT32_MAX;

	return ClassOf(size);
}

bool TlsMemoryPool::Initialize(const SlabConfig* configs, uint32_t configCount, uint32_t payloadAlignment)
{
	if (m_initialized)
		return false;

	if (!configs || configCount == 0)
	{
		LOGE("Initialize called with no configuration");
		return false;
	}

	if (!IsPowerOfTwo(payloadAlignment) || payloadAlignment < 16 || payloadAlignment > 4096)
	{
		LOGE("payload alignment %u is invalid. it must be a power of two between 16 and 4096", payloadAlignment);
		return false;
	}

	// 1. 설정에서 클래스 범위를 뽑는다.
	//    가장 작은 blockSize 를 담는 클래스가 0 번이 되고,
	//    가장 큰 blockSize 를 담는 클래스가 마지막이 된다.
	size_t smallest = SIZE_MAX;
	size_t largest = 0;

	for (uint32_t i = 0; i < configCount; ++i)
	{
		if (configs[i].blockSize == 0)
		{
			LOGE("configuration %u has a zero block size", i);
			return false;
		}

		if (configs[i].blockSize < smallest) smallest = configs[i].blockSize;
		if (configs[i].blockSize > largest)  largest = configs[i].blockSize;
	}

	if (smallest < MIN_BLOCK_SIZE)
	{
		// 묶음 머리 정보를 블록 페이로드에 얹기 때문에 최소 크기가 필요하다.
		LOGW("smallest configured block size %zu is below the minimum %u, raising it",
			smallest, MIN_BLOCK_SIZE);
		smallest = MIN_BLOCK_SIZE;
	}

	m_minClassLog = CeilLog2(smallest);
	const uint32_t maxClassLog = CeilLog2(largest);
	m_classCount = maxClassLog - m_minClassLog + 1;

	if (m_classCount > MAX_CLASS_COUNT)
	{
		LOGE("configuration spans %u size classes (%zu .. %zu) but the maximum is %u",
			m_classCount, smallest, largest, MAX_CLASS_COUNT);
		return false;
	}

	m_minMask = (static_cast<size_t>(1) << m_minClassLog) - 1;
	m_payloadAlignment = payloadAlignment;
	m_headerSize = static_cast<uint32_t>(AlignUpSize(sizeof(BlockHeader), payloadAlignment));

	// 2. 풀 슬롯을 하나 점유한다.
	// 빠른 슬롯을 못 받아도 초기화를 실패시키지 않는다.
	// 메모리 풀이 "자리가 없어서" 못 만들어지면 그 위의 모든 것이 무너진다.
	// 이 경우 thread_local 배열 대신 FLS 조회로 스레드 캐시를 찾는다.
	// 함수 호출 한 번이 더 붙을 뿐 동작과 락 특성은 완전히 같다.
	m_poolId = AcquirePoolSlot();
	if (m_poolId == UINT32_MAX)
	{
		LOGW("no fast pool slot left (maximum %u per process), falling back to FLS lookup for thread caches", MAX_POOL_COUNT);
	}

	m_generation = static_cast<uint32_t>(::InterlockedIncrement(&g_generationCounter));

	// 3. 스레드 종료 콜백용 FLS 슬롯.
	//    이게 없으면 스레드가 죽을 때 그 스레드 캐시에 남은 블록이 그대로 샌다.
	m_flsIndex = ::FlsAlloc(&TlsMemoryPool::ThreadCacheDestructor);
	if (m_flsIndex == FLS_OUT_OF_INDEXES)
	{
		LOGE("FlsAlloc failed (error %lu). thread caches would leak on thread exit", ::GetLastError());
		ReleasePoolSlot(m_poolId);
		m_poolId = UINT32_MAX;
		return false;
	}

	HANDLE heap = ::GetProcessHeap();

	// 4. 클래스 서술자
	m_classes = static_cast<ClassDesc*>(::HeapAlloc(heap, HEAP_ZERO_MEMORY, sizeof(ClassDesc) * m_classCount));
	if (!m_classes)
	{
		Finalize();
		return false;
	}

	for (uint32_t c = 0; c < m_classCount; ++c)
	{
		ClassDesc& desc = m_classes[c];

		desc.blockSize = static_cast<uint32_t>(static_cast<size_t>(1) << (m_minClassLog + c));
		desc.stride = static_cast<uint32_t>(AlignUpSize(m_headerSize + desc.blockSize, payloadAlignment));

		// 개수가 아니라 바이트로 상한을 잡는다. 큰 클래스가 스레드 캐시를
		// 통째로 삼켜 L2 를 밀어내는 것을 막는 게 목적이다.
		uint32_t cap = static_cast<uint32_t>(TLS_CACHE_BYTES_PER_CLASS / desc.blockSize);
		if (cap > TLS_CAP_BLOCK_LIMIT) cap = TLS_CAP_BLOCK_LIMIT;
		if (cap < TLS_CAP_MIN_BLOCKS) cap = 0;

		desc.tlsCapBlocks = cap;
		desc.batchBlocks = (cap >= 4) ? (cap / 4) : 1;

		size_t blocksPerSpan = SPAN_TARGET_BYTES / desc.stride;
		if (blocksPerSpan < static_cast<size_t>(desc.batchBlocks) * 2)
			blocksPerSpan = static_cast<size_t>(desc.batchBlocks) * 2;
		if (blocksPerSpan < 2)
			blocksPerSpan = 2;

		desc.blocksPerSpan = static_cast<uint32_t>(blocksPerSpan);
	}

	// 5. 전역 저장소. SLIST_HEADER 와 false sharing 회피를 위해 64바이트 정렬이 필요한데
	//    HeapAlloc 은 16바이트까지만 보장하므로 직접 맞춘다.
	const size_t globalsBytes = sizeof(GlobalClass) * m_classCount + 64;
	m_globalsRaw = ::HeapAlloc(heap, HEAP_ZERO_MEMORY, globalsBytes);
	if (!m_globalsRaw)
	{
		Finalize();
		return false;
	}

	m_globals = reinterpret_cast<GlobalClass*>(
		AlignUpSize(reinterpret_cast<uintptr_t>(m_globalsRaw), 64));

	for (uint32_t c = 0; c < m_classCount; ++c)
	{
		::InitializeSListHead(&m_globals[c].chunkList);
		::InitializeSRWLock(&m_globals[c].growLock);

		// 아직 한 번도 꺼내가지 않았다는 뜻. 첫 Refill 에서 실제 값으로 내려간다.
		m_globals[c].minBlocksInGlobal = MAXLONGLONG;
	}

	// 6. 종료된 스레드의 지표를 받아둘 자리
	m_retiredAcquire = static_cast<uint64_t*>(::HeapAlloc(heap, HEAP_ZERO_MEMORY, sizeof(uint64_t) * MAX_CLASS_COUNT));
	m_retiredRelease = static_cast<uint64_t*>(::HeapAlloc(heap, HEAP_ZERO_MEMORY, sizeof(uint64_t) * MAX_CLASS_COUNT));

	if (!m_retiredAcquire || !m_retiredRelease)
	{
		Finalize();
		return false;
	}

	m_initialized = true;

	// 7. 설정에 적힌 만큼 미리 만들어 둔다.
	uint32_t requested[MAX_CLASS_COUNT] = {};
	for (uint32_t i = 0; i < configCount; ++i)
	{
		const uint32_t c = ClassOf(configs[i].blockSize);
		if (c < m_classCount)
			requested[c] += configs[i].blockCount;
	}

	for (uint32_t c = 0; c < m_classCount; ++c)
	{
		if (requested[c] == 0)
			continue;

		GlobalClass& global = m_globals[c];
		::AcquireSRWLockExclusive(&global.growLock);
		const bool ok = AllocateSpanLocked(c, requested[c], true);
		::ReleaseSRWLockExclusive(&global.growLock);

		if (!ok)
		{
			LOGE("failed to preallocate %u blocks for class %u (block size %u)",
				requested[c], c, m_classes[c].blockSize);
			Finalize();
			return false;
		}
	}

	LOGI("pool %u initialized : %u size classes (%u .. %u bytes), payload alignment %u, header %u",
		m_poolId, m_classCount, m_classes[0].blockSize,
		m_classes[m_classCount - 1].blockSize, m_payloadAlignment, m_headerSize);

	return true;
}

void TlsMemoryPool::Finalize()
{
	if (m_poolId == UINT32_MAX && !m_classes && !m_globals)
		return;

	// 1. 스레드 종료 콜백을 먼저 닫는다.
	//    아래에서 캐시를 해제한 뒤에 콜백이 돌면 해제된 메모리를 만지게 된다.
	if (m_flsIndex != FLS_OUT_OF_INDEXES)
	{
		::FlsFree(m_flsIndex);
		m_flsIndex = FLS_OUT_OF_INDEXES;
	}

	// 2. 남아있는 스레드 캐시를 모두 회수한다.
	//    이 시점에 캐시가 남아 있다는 것은 그 스레드가 아직 살아 있다는 뜻이므로,
	//    호출 계약(모든 워커 종료 후 Finalize)이 지켜졌는지 확인하는 지표이기도 하다.
	for (;;)
	{
		::AcquireSRWLockExclusive(&m_registryLock);
		ThreadCache* cache = m_cacheList;
		::ReleaseSRWLockExclusive(&m_registryLock);

		if (!cache)
			break;

		DetachThreadCache(cache);
	}

	if (m_initialized && m_classes && m_globals && m_retiredAcquire && m_retiredRelease)
	{
		LogStats("finalize");

		uint64_t totalAcquire = 0;
		uint64_t totalRelease = 0;
		for (uint32_t c = 0; c < m_classCount; ++c)
		{
			totalAcquire += m_retiredAcquire[c];
			totalRelease += m_retiredRelease[c];
		}

		const LONG64 bypassAcquire = ::InterlockedCompareExchange64(&m_bypassAcquire, 0, 0);
		const LONG64 bypassRelease = ::InterlockedCompareExchange64(&m_bypassRelease, 0, 0);

		if (totalAcquire != totalRelease || bypassAcquire != bypassRelease)
		{
			LOGE("finalize with unreleased blocks : pooled %llu outstanding, bypass %lld outstanding. see the stats above",
				totalAcquire - totalRelease, bypassAcquire - bypassRelease);
			ENGINE_BREAK_IF_DEBUGGER();
		}
	}

	HANDLE heap = ::GetProcessHeap();

	// 3. 스팬 반납
	if (m_globals && m_classes)
	{
		for (uint32_t c = 0; c < m_classCount; ++c)
		{
			GlobalClass& global = m_globals[c];

			for (uint32_t s = 0; s < global.spanCount; ++s)
			{
				if (global.spans[s])
					::VirtualFree(global.spans[s], 0, MEM_RELEASE);
			}

			if (global.spans)     ::HeapFree(heap, 0, global.spans);
			if (global.spanBytes) ::HeapFree(heap, 0, global.spanBytes);

			global.spans = nullptr;
			global.spanBytes = nullptr;
			global.spanCount = 0;
			global.spanCapacity = 0;
		}
	}

	if (m_globalsRaw)     { ::HeapFree(heap, 0, m_globalsRaw);     m_globalsRaw = nullptr; }
	if (m_classes)        { ::HeapFree(heap, 0, m_classes);        m_classes = nullptr; }
	if (m_retiredAcquire) { ::HeapFree(heap, 0, m_retiredAcquire); m_retiredAcquire = nullptr; }
	if (m_retiredRelease) { ::HeapFree(heap, 0, m_retiredRelease); m_retiredRelease = nullptr; }

	m_globals = nullptr;

	if (m_poolId != UINT32_MAX)
	{
		ReleasePoolSlot(m_poolId);
		m_poolId = UINT32_MAX;
	}

	// 세대를 무효화해서 남아있는 TLS 슬롯이 이 풀의 캐시를 가리키지 않게 한다.
	m_generation = 0;
	m_classCount = 0;
	m_initialized = false;
}

TlsMemoryPool::ThreadCache* TlsMemoryPool::GetThreadCache()
{
	if (m_poolId < MAX_POOL_COUNT)
	{
		TlsSlot& slot = t_slots[m_poolId];
		if (slot.generation == m_generation)
			return static_cast<ThreadCache*>(slot.cache);
	}
	else if (m_flsIndex != FLS_OUT_OF_INDEXES)
	{
		ThreadCache* cache = static_cast<ThreadCache*>(::FlsGetValue(m_flsIndex));
		if (cache)
			return cache;
	}

	return AttachThreadCache();
}

TlsMemoryPool::ThreadCache* TlsMemoryPool::AttachThreadCache()
{
	ThreadCache* cache = static_cast<ThreadCache*>(
		::HeapAlloc(::GetProcessHeap(), HEAP_ZERO_MEMORY, sizeof(ThreadCache)));

	if (!cache)
	{
		LOGE("pool %u failed to allocate a thread cache for thread %lu", m_poolId, ::GetCurrentThreadId());
		return nullptr;
	}

	cache->owner = this;
	cache->poolId = m_poolId;
	cache->classCount = m_classCount;
	cache->threadId = ::GetCurrentThreadId();

	::AcquireSRWLockExclusive(&m_registryLock);
	cache->nextRegistered = m_cacheList;
	m_cacheList = cache;
	::ReleaseSRWLockExclusive(&m_registryLock);

	if (m_poolId < MAX_POOL_COUNT)
	{
		t_slots[m_poolId].cache = cache;
		t_slots[m_poolId].generation = m_generation;
	}

	// 스레드가 죽을 때 이 캐시를 회수하기 위한 등록. 스레드당 한 번만 호출된다.
	if (m_flsIndex != FLS_OUT_OF_INDEXES)
		::FlsSetValue(m_flsIndex, cache);

	return cache;
}

void TlsMemoryPool::DetachThreadCache(ThreadCache* cache)
{
	if (!cache)
		return;

	// 스레드 종료 콜백과 Finalize 가 같은 캐시를 두고 겹칠 수 있다.
	if (::InterlockedCompareExchange(&cache->detached, 1, 0) != 0)
		return;

	// 남은 블록을 전역으로 되돌린다. 이걸 빼먹으면 그대로 누수다.
	for (uint32_t c = 0; c < cache->classCount; ++c)
		FlushBin(cache, c, true);

	::AcquireSRWLockExclusive(&m_registryLock);

	ThreadCache** link = &m_cacheList;
	while (*link)
	{
		if (*link == cache)
		{
			*link = cache->nextRegistered;
			break;
		}
		link = &(*link)->nextRegistered;
	}

	if (m_retiredAcquire && m_retiredRelease)
	{
		for (uint32_t c = 0; c < cache->classCount; ++c)
		{
			m_retiredAcquire[c] += cache->acquireCount[c];
			m_retiredRelease[c] += cache->releaseCount[c];
		}
	}

	::ReleaseSRWLockExclusive(&m_registryLock);

	// 이 스레드의 슬롯이 방금 회수한 캐시를 가리키고 있으면 지운다.
	// (Finalize 가 다른 스레드의 캐시를 회수하는 경우에는 일치하지 않는다)
	if (cache->poolId < MAX_POOL_COUNT && t_slots[cache->poolId].cache == cache)
	{
		t_slots[cache->poolId].cache = nullptr;
		t_slots[cache->poolId].generation = 0;
	}

	// FLS 조회 경로를 쓰는 풀이면 해제한 포인터가 남지 않도록 지운다.
	// (다른 스레드가 대신 회수하는 경우에는 그 스레드의 슬롯을 건드리면 안 된다)
	if (m_flsIndex != FLS_OUT_OF_INDEXES && cache->threadId == ::GetCurrentThreadId())
		::FlsSetValue(m_flsIndex, nullptr);

	::HeapFree(::GetProcessHeap(), 0, cache);
}

void WINAPI TlsMemoryPool::ThreadCacheDestructor(PVOID value)
{
	ThreadCache* cache = static_cast<ThreadCache*>(value);
	if (!cache || !cache->owner)
		return;

	cache->owner->DetachThreadCache(cache);
}

bool TlsMemoryPool::RefillBin(ThreadCache* cache, uint32_t classIndex)
{
	GlobalClass& global = m_globals[classIndex];

	PSLIST_ENTRY entry = ::InterlockedPopEntrySList(&global.chunkList);

	if (!entry)
	{
		// 전역이 비었다. 여기서만 락을 잡는다.
		::AcquireSRWLockExclusive(&global.growLock);

		// 락을 기다리는 동안 다른 스레드가 채웠을 수 있다.
		entry = ::InterlockedPopEntrySList(&global.chunkList);
		if (!entry)
		{
			if (AllocateSpanLocked(classIndex, m_classes[classIndex].batchBlocks, false))
				entry = ::InterlockedPopEntrySList(&global.chunkList);
		}

		::ReleaseSRWLockExclusive(&global.growLock);
	}

	if (!entry)
		return false;

	ChunkNode* node = reinterpret_cast<ChunkNode*>(entry);
	BlockHeader* head = node->blockHead;
	const uint32_t count = node->blockCount;

	const LONG64 remaining =
		::InterlockedExchangeAdd64(&global.blocksInGlobal, -static_cast<LONG64>(count)) - static_cast<LONG64>(count);

	// 전역 재고의 최저점을 갱신한다. batch 개마다 한 번만 도는 경로라
	// CAS 루프를 둬도 부담이 없고, 이 값이 풀 크기를 정하는 유일한 근거다.
	for (;;)
	{
		const LONG64 current = ::InterlockedCompareExchange64(&global.minBlocksInGlobal, 0, 0);
		if (remaining >= current)
			break;
		if (::InterlockedCompareExchange64(&global.minBlocksInGlobal, remaining, current) == current)
			break;
	}

	// bin 이 비었을 때만 이 함수를 부르므로 이어붙일 필요가 없다.
	Bin& bin = cache->bins[classIndex];
	bin.head = head;
	bin.count = count;

	return true;
}

void TlsMemoryPool::FlushBin(ThreadCache* cache, uint32_t classIndex, bool flushAll)
{
	Bin& bin = cache->bins[classIndex];
	if (bin.count == 0 || bin.head == nullptr)
		return;

	GlobalClass& global = m_globals[classIndex];
	const uint32_t batch = m_classes[classIndex].batchBlocks;

	do
	{
		uint32_t take = batch;
		if (take > bin.count)
			take = bin.count;

		if (take == 0)
			break;

		// 묶음의 꼬리를 찾는다. take-1 번 링크를 따라간다.
		// 이 비용은 take 번의 해제마다 한 번이므로 해제 1건당 링크 추적 1회로
		// 상각된다. 대신 없어진 것이 배타 락 획득이다.
		BlockHeader* head = bin.head;
		BlockHeader* tail = head;
		for (uint32_t i = 1; i < take; ++i)
			tail = tail->next;

		bin.head = tail->next;
		bin.count -= take;
		tail->next = nullptr;

		ChunkNode* node = static_cast<ChunkNode*>(PayloadOf(head));
		node->blockHead = head;
		node->blockCount = take;

		::InterlockedPushEntrySList(&global.chunkList, &node->entry);
		::InterlockedExchangeAdd64(&global.blocksInGlobal, static_cast<LONG64>(take));

	} while (flushAll && bin.count > 0);
}

bool TlsMemoryPool::RecordSpanLocked(GlobalClass& global, void* span, size_t spanBytes)
{
	HANDLE heap = ::GetProcessHeap();

	if (global.spanCount >= global.spanCapacity)
	{
		const uint32_t newCapacity = (global.spanCapacity == 0) ? 4 : global.spanCapacity * 2;

		void** newSpans = static_cast<void**>(::HeapAlloc(heap, HEAP_ZERO_MEMORY, sizeof(void*) * newCapacity));
		if (!newSpans)
			return false;

		size_t* newBytes = static_cast<size_t*>(::HeapAlloc(heap, HEAP_ZERO_MEMORY, sizeof(size_t) * newCapacity));
		if (!newBytes)
		{
			::HeapFree(heap, 0, newSpans);
			return false;
		}

		if (global.spans)
		{
			::memcpy(newSpans, global.spans, sizeof(void*) * global.spanCount);
			::memcpy(newBytes, global.spanBytes, sizeof(size_t) * global.spanCount);
			::HeapFree(heap, 0, global.spans);
			::HeapFree(heap, 0, global.spanBytes);
		}

		global.spans = newSpans;
		global.spanBytes = newBytes;
		global.spanCapacity = newCapacity;
	}

	global.spans[global.spanCount] = span;
	global.spanBytes[global.spanCount] = spanBytes;
	++global.spanCount;

	return true;
}

bool TlsMemoryPool::AllocateSpanLocked(uint32_t classIndex, uint32_t minBlocks, bool isInitial)
{
	ClassDesc& desc = m_classes[classIndex];
	GlobalClass& global = m_globals[classIndex];

	uint32_t blocks = desc.blocksPerSpan;
	if (blocks < minBlocks)
		blocks = minBlocks;

	const size_t spanBytes = static_cast<size_t>(desc.stride) * blocks;

	// VirtualAlloc 은 64KB 경계로 정렬된 주소를 준다.
	// stride 와 headerSize 가 payloadAlignment 의 배수이므로
	// 모든 블록의 페이로드가 payloadAlignment 로 정렬된다.
	void* span = ::VirtualAlloc(nullptr, spanBytes, MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE);
	if (!span)
	{
		LOGE("pool %u class %u (block size %u) failed to reserve a %zu byte span (error %lu)",
			m_poolId, classIndex, desc.blockSize, spanBytes, ::GetLastError());
		::InterlockedIncrement(&global.acquireFailCount);
		return false;
	}

	if (!RecordSpanLocked(global, span, spanBytes))
	{
		::VirtualFree(span, 0, MEM_RELEASE);
		return false;
	}

	// 스팬을 batch 크기 묶음으로 잘라 전역 리스트에 올린다.
	char* cursor = static_cast<char*>(span);
	uint32_t remaining = blocks;

	while (remaining > 0)
	{
		uint32_t take = desc.batchBlocks;
		if (take > remaining)
			take = remaining;

		BlockHeader* head = nullptr;
		for (uint32_t i = 0; i < take; ++i)
		{
			BlockHeader* header = reinterpret_cast<BlockHeader*>(cursor + static_cast<size_t>(desc.stride) * i);
			header->magic = MAGIC_FREE;
			header->classIndex = static_cast<uint8_t>(classIndex);
			header->poolId = OwnerTag();
			header->reserved = 0;
			header->next = head;
			head = header;
		}

		cursor += static_cast<size_t>(desc.stride) * take;
		remaining -= take;

		ChunkNode* node = static_cast<ChunkNode*>(PayloadOf(head));
		node->blockHead = head;
		node->blockCount = take;

		::InterlockedPushEntrySList(&global.chunkList, &node->entry);
		::InterlockedExchangeAdd64(&global.blocksInGlobal, static_cast<LONG64>(take));
	}

	::InterlockedExchangeAdd64(&global.blocksCreated, static_cast<LONG64>(blocks));

	if (!isInitial)
	{
		// 초기 blockCount 가 부족했다는 신호다. 튜닝 근거로 남긴다.
		::InterlockedIncrement(&global.growthCount);
		LOGW("pool %u class %u (block size %u) grew at runtime : +%u blocks, %lld total",
			m_poolId, classIndex, desc.blockSize, blocks,
			::InterlockedCompareExchange64(&global.blocksCreated, 0, 0));
	}

	return true;
}

void* TlsMemoryPool::Acquire(size_t size)
{
	if (!m_initialized || size == 0)
		return nullptr;

	const uint32_t classIndex = ClassOf(size);
	if (classIndex >= m_classCount)
		return AcquireBypass(size);

	ThreadCache* cache = GetThreadCache();
	if (!cache)
		return nullptr;

	Bin& bin = cache->bins[classIndex];

	if (bin.head == nullptr)
	{
		if (!RefillBin(cache, classIndex))
		{
			::InterlockedIncrement(&m_globals[classIndex].acquireFailCount);
			LOGE("pool %u class %u (block size %u) exhausted, cannot satisfy a %zu byte request",
				m_poolId, classIndex, m_classes[classIndex].blockSize, size);
			return nullptr;
		}
	}

	BlockHeader* header = bin.head;
	bin.head = header->next;
	--bin.count;

	if (header->magic != MAGIC_FREE)
	{
		ENGINE_CORRUPTION("pool %u free list holds a block with magic 0x%08X (expected 0x%08X) at %p. the block was overrun or handed out twice",
			m_poolId, header->magic, MAGIC_FREE, static_cast<void*>(header));
		return nullptr;
	}

	header->magic = MAGIC_LIVE;
	header->next = nullptr;

	++cache->acquireCount[classIndex];

	return PayloadOf(header);
}

void TlsMemoryPool::Release(const void* payload)
{
	if (!payload)
		return;

	if (!m_initialized)
	{
		ENGINE_VIOLATION("pool %u Release called after finalize (payload %p)", m_poolId, payload);
		return;
	}

	BlockHeader* header = HeaderOf(payload);

	if (header->magic == MAGIC_FREE)
	{
		ENGINE_CORRUPTION("pool %u double free : payload %p is already on a free list", m_poolId, payload);
		return;
	}

	if (header->magic != MAGIC_LIVE)
	{
		ENGINE_CORRUPTION("pool %u block header corrupted : payload %p, magic 0x%08X (expected 0x%08X). overrun or a pointer that did not come from this pool",
			m_poolId, payload, header->magic, MAGIC_LIVE);
		return;
	}

	// 빠른 슬롯을 못 받은 풀들은 서로 구분할 수 없으므로 이 검사를 건너뛴다.
	if (header->poolId != POOL_ID_NONE && header->poolId != OwnerTag())
	{
		ENGINE_VIOLATION("payload %p belongs to pool %u but was released to pool %u",
			payload, header->poolId, m_poolId);
		return;
	}

	header->magic = MAGIC_FREE;

	if (header->classIndex == CLASS_BYPASS)
	{
		ReleaseBypass(header);
		return;
	}

	const uint32_t classIndex = header->classIndex;
	if (classIndex >= m_classCount)
	{
		ENGINE_CORRUPTION("pool %u block header has class index %u but the pool has %u classes : payload %p",
			m_poolId, classIndex, m_classCount, payload);
		return;
	}

	// 다른 스레드가 할당한 블록이어도 그냥 이 스레드의 bin 으로 넣는다.
	// 블록에 주인이 없으므로 이게 성립한다. 생산 스레드와 소비 스레드가
	// 나뉜 구조에서는 블록이 한쪽으로 흐르는데, 그건 아래 flush 가 잡아준다.
	ThreadCache* cache = GetThreadCache();

	if (!cache)
	{
		// 캐시를 못 만들었으면 블록 하나짜리 묶음으로 전역에 직접 돌려준다.
		// 여기서 포기하면 블록이 그대로 샌다.
		GlobalClass& global = m_globals[classIndex];
		header->next = nullptr;

		ChunkNode* node = static_cast<ChunkNode*>(PayloadOf(header));
		node->blockHead = header;
		node->blockCount = 1;

		::InterlockedPushEntrySList(&global.chunkList, &node->entry);
		::InterlockedExchangeAdd64(&global.blocksInGlobal, 1);
		return;
	}

	Bin& bin = cache->bins[classIndex];
	header->next = bin.head;
	bin.head = header;
	++bin.count;

	++cache->releaseCount[classIndex];

	if (bin.count > m_classes[classIndex].tlsCapBlocks)
		FlushBin(cache, classIndex, false);
}

void* TlsMemoryPool::AcquireBypass(size_t size)
{
	// 최대 클래스를 넘는 요청. 풀링하지 않고 OS 에서 직접 받는다.
	// 헤더는 그대로 붙이므로 Release 는 우회 블록인지 여부만 보면 된다.
	const size_t rawBytes = m_headerSize + size + m_payloadAlignment;

	void* raw = ::HeapAlloc(::GetProcessHeap(), 0, rawBytes);
	if (!raw)
	{
		LOGE("pool %u bypass allocation of %zu bytes failed", m_poolId, size);
		return nullptr;
	}

	char* payload = reinterpret_cast<char*>(
		AlignUpSize(reinterpret_cast<uintptr_t>(raw) + m_headerSize, m_payloadAlignment));

	BlockHeader* header = reinterpret_cast<BlockHeader*>(payload - m_headerSize);
	header->magic = MAGIC_LIVE;
	header->classIndex = CLASS_BYPASS;
	header->poolId = OwnerTag();
	header->reserved = 0;
	header->next = static_cast<BlockHeader*>(raw);   // 해제용 원본 주소

	::InterlockedIncrement64(&m_bypassAcquire);

	return payload;
}

void TlsMemoryPool::ReleaseBypass(BlockHeader* header)
{
	void* raw = header->next;
	::InterlockedIncrement64(&m_bypassRelease);
	::HeapFree(::GetProcessHeap(), 0, raw);
}

bool TlsMemoryPool::GetSlabStats(uint32_t classIndex, SlabStats& outStats) const
{
	if (!m_initialized || classIndex >= m_classCount)
		return false;

	const ClassDesc& desc = m_classes[classIndex];
	GlobalClass& global = m_globals[classIndex];

	outStats = SlabStats{};
	outStats.blockSize = desc.blockSize;
	outStats.tlsCapBlocks = desc.tlsCapBlocks;
	outStats.batchBlocks = desc.batchBlocks;

	outStats.blockCount = static_cast<uint32_t>(::InterlockedCompareExchange64(&global.blocksCreated, 0, 0));
	outStats.blocksInGlobal = static_cast<uint32_t>(::InterlockedCompareExchange64(&global.blocksInGlobal, 0, 0));
	outStats.growthCount = static_cast<uint32_t>(::InterlockedCompareExchange(&global.growthCount, 0, 0));
	outStats.acquireFailCount = static_cast<uint32_t>(::InterlockedCompareExchange(&global.acquireFailCount, 0, 0));

	uint64_t acquire = m_retiredAcquire[classIndex];
	uint64_t release = m_retiredRelease[classIndex];
	uint64_t inTls = 0;

	::AcquireSRWLockShared(&m_registryLock);
	for (ThreadCache* cache = m_cacheList; cache != nullptr; cache = cache->nextRegistered)
	{
		acquire += cache->acquireCount[classIndex];
		release += cache->releaseCount[classIndex];
		inTls += cache->bins[classIndex].count;
	}
	::ReleaseSRWLockShared(&m_registryLock);

	outStats.totalAcquire = acquire;
	outStats.totalRelease = release;
	outStats.allocatedCount = static_cast<uint32_t>(acquire - release);

	// 전역 재고가 가장 적었던 순간에 밖에 나가 있던 블록 수.
	// 그 중 일부는 스레드 캐시에 놀고 있었을 수 있으므로 상한이다.
	const LONG64 minInGlobal = ::InterlockedCompareExchange64(&global.minBlocksInGlobal, 0, 0);
	outStats.peakAllocated = (minInGlobal >= 0 && minInGlobal <= static_cast<LONG64>(outStats.blockCount))
		? static_cast<uint32_t>(static_cast<LONG64>(outStats.blockCount) - minInGlobal)
		: 0;
	outStats.blocksInTls = static_cast<uint32_t>(inTls);

	return true;
}

void TlsMemoryPool::LogStats(const char* poolName) const
{
	if (!m_initialized)
		return;

	const char* name = poolName ? poolName : "pool";

	for (uint32_t c = 0; c < m_classCount; ++c)
	{
		SlabStats stats;
		if (!GetSlabStats(c, stats))
			continue;

		// 아무 일도 없었던 클래스는 건너뛴다. 로그가 의미 있는 줄만 남게 한다.
		if (stats.totalAcquire == 0 && stats.blockCount == 0)
			continue;

		const char* marker = (stats.allocatedCount != 0) ? " OUTSTANDING" : "";

		LOGI("[%s] pool %u class %u : size %6u | created %6u  global %6u  tls %6u  live %6u  peak %6u | acquire %llu  release %llu | growth %u  fail %u | cap %u batch %u%s",
			name, m_poolId, c, stats.blockSize,
			stats.blockCount, stats.blocksInGlobal, stats.blocksInTls,
			stats.allocatedCount, stats.peakAllocated,
			stats.totalAcquire, stats.totalRelease,
			stats.growthCount, stats.acquireFailCount,
			stats.tlsCapBlocks, stats.batchBlocks, marker);
	}

	const LONG64 bypassAcquire = ::InterlockedCompareExchange64(&m_bypassAcquire, 0, 0);
	if (bypassAcquire != 0)
	{
		const LONG64 bypassRelease = ::InterlockedCompareExchange64(&m_bypassRelease, 0, 0);
		LOGI("[%s] pool %u bypass (over the largest class) : acquire %lld  release %lld  live %lld",
			name, m_poolId, bypassAcquire, bypassRelease, bypassAcquire - bypassRelease);
	}
}
