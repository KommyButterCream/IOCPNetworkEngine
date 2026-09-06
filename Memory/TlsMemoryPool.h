#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>
#include <stdint.h>

#ifndef IOCP_ENGINE_API
#ifdef BUILD_IOCP_ENGINE_DLL
#define IOCP_ENGINE_API __declspec(dllexport)
#else
#define IOCP_ENGINE_API __declspec(dllimport)
#endif
#endif

// TLS 2단계 크기별 메모리 풀
//
// SlabMemoryPool 을 대체한다. 공개 API 는 의도적으로 동일하게 유지했으므로
// 호출부는 타입 이름만 바꾸면 된다. (payloadAlignment 인자만 새로 생겼고
// 기본값이 있으므로 기존 호출은 그대로 컴파일된다)
//
// 구조
//
//   요청 크기
//      |
//      +--> [0] 크기 클래스 산출     BitScanReverse 1 명령. 메모리 접근 없음
//      |
//      +--> [1] ThreadCache (TLS)    락 0. 스레드 로컬 단일 연결 리스트
//      |         비었으면 v / 넘치면 v
//      +--> [2] GlobalClass          SLIST_HEADER. 락 0. batch 개 묶음 단위
//      |         비었으면 v
//      +--> [3] Span                 VirtualAlloc. SRWLOCK. 정상 운영 중엔 호출되지 않음
//      |
//      +--> [4] 최대 클래스 초과      HeapAlloc 우회
//
// 이전 SlabMemoryPool 대비 달라진 점
//
//   - 크기 클래스 결정이 슬랩 배열 선형 탐색(최대 20 캐시라인 접근)에서
//     BitScanReverse 한 번(메모리 접근 0)으로 바뀌었다. 탐색이 훑던 라인들은
//     다른 스레드가 계속 쓰는 라인이라 무경합 상황에서도 캐시 핑퐁을 만들었다.
//   - Acquire / Release 경로에 락이 하나도 없다. 전역 접근은 batch 개마다
//     한 번이고 그마저 Interlocked SLIST 다.
//   - payloadAlignment 를 풀마다 지정한다. Job 풀은 64 를 줘야 한다.
//     기존 풀은 페이로드를 16바이트 정렬로만 잡아서 align(64) 인 Job 이
//     실제로는 4개 중 1개만 정렬되어 있었다.
//
// 스레드 수명
//
//   스레드가 처음 Acquire / Release 를 호출할 때 그 스레드의 ThreadCache 가
//   만들어지고, 스레드가 끝날 때 FLS 콜백이 캐시에 남은 블록을 전역으로
//   되돌린다. 콜백이 없으면 그 블록들은 그대로 누수된다.
//
// 종료 계약
//
//   Finalize 는 이 풀을 사용하던 모든 스레드가 종료되었거나 최소한 더 이상
//   할당하지 않는 시점에 호출해야 한다. 엔진은 워커 스레드를 join 한 뒤
//   풀을 지우므로 이 계약을 이미 만족한다.

class IOCP_ENGINE_API TlsMemoryPool
{
	// Define Structure
public:
	// 메모리 풀 초기화에 사용. SlabMemoryPool::SlabConfig 와 같은 모양이다.
	//
	// blockSize 는 이제 "정확한 슬랩 크기" 가 아니라 "이 크기를 담을 수 있는
	// 2의 거듭제곱 클래스를 만들어라" 는 뜻이다. {64,...} 와 {128,...} 을 같이
	// 주면 64 는 클래스 0(64B), 128 은 클래스 1(128B) 이 된다.
	// blockCount 는 그 클래스에 미리 만들어 둘 블록 수다.
	struct SlabConfig
	{
		uint32_t blockSize = 0;
		uint32_t blockCount = 0;
	};

	// 클래스별 운영 지표.
	// SlabMemoryPool::SlabStats 와 이름이 겹치는 필드는 의미도 같게 두었다.
	//
	//   blocksCreated  : 스팬에서 실제로 만들어낸 블록 수 (정확값)
	//   growthCount    : 스팬 확장 횟수. 0 이 아니면 초기 blockCount 가 부족했다
	//   allocatedCount : 지금 사용 중인 블록 수 (정확값, totalAcquire-totalRelease)
	//   peakAllocated  : 스레드별 최고치의 합. 실제 동시 최고치의 상한이다
	//                    (스레드들의 peak 시점이 다를 수 있으므로 정확값이 아니다)
	//   blocksInGlobal : 전역 SLIST 에 들어있는 블록 수
	//   blocksInTls    : 살아있는 스레드 캐시들이 들고 있는 블록 수
	struct SlabStats
	{
		uint32_t blockSize = 0;
		uint32_t blockCount = 0;        // = blocksCreated. 기존 필드명 호환용
		uint32_t allocatedCount = 0;
		uint32_t peakAllocated = 0;
		uint32_t growthCount = 0;
		uint32_t acquireFailCount = 0;
		uint64_t totalAcquire = 0;
		uint64_t totalRelease = 0;

		uint32_t blocksInGlobal = 0;
		uint32_t blocksInTls = 0;
		uint32_t tlsCapBlocks = 0;
		uint32_t batchBlocks = 0;
	};

	// thread_local 배열로 빠르게 찾을 수 있는 풀 개수.
	// 엔진 인스턴스 하나가 packet / job / general 3개를 쓰는데,
	// 한 프로세스가 서버 1개와 클라이언트 N개를 동시에 띄우면 3 x (N+1) 개가 된다.
	// 이 수를 넘어도 실패하지 않고 FLS 조회로 물러난다(느릴 뿐 동작은 같다).
	static constexpr uint32_t MAX_POOL_COUNT = 64;

	// 빠른 슬롯을 받지 못한 풀의 블록 표시. 이 값이면 소유 풀 검사를 건너뛴다.
	static constexpr uint8_t POOL_ID_NONE = 0xFFu;

	// 지원하는 크기 클래스 최대 개수. 32B ~ 32B<<23 까지 커버한다.
	static constexpr uint32_t MAX_CLASS_COUNT = 24;

	// 가장 작은 클래스의 크기. ChunkNode 를 블록 페이로드에 얹기 때문에
	// 블록이 최소 이 크기는 되어야 한다.
	static constexpr uint32_t MIN_BLOCK_SIZE = 32;

	// 클래스 하나가 스레드 캐시에서 차지할 수 있는 최대 바이트.
	// 블록 개수 상한이 아니라 바이트 상한이라는 점이 중요하다.
	// 개수로 고정하면(예: 전 클래스 128개) 64KB 클래스만으로 스레드당 8MB 를
	// 잡아먹어서 L2 를 통째로 밀어낸다. 캐시 적중률이 목적인데 그러면 역효과다.
	static constexpr uint32_t TLS_CACHE_BYTES_PER_CLASS = 64 * 1024;

	// 블록 헤더 매직. 살아있는 블록과 반환된 블록을 다른 값으로 표시해서
	// 이중 해제를 그 자리에서 잡는다.
	static constexpr uint32_t MAGIC_LIVE = 0xA110C8EDu;
	static constexpr uint32_t MAGIC_FREE = 0xF2EEB10Cu;

	// 최대 클래스를 넘어 OS 로 우회한 블록 표시
	static constexpr uint8_t CLASS_BYPASS = 0xFFu;

public:
	TlsMemoryPool();
	~TlsMemoryPool();

	TlsMemoryPool(const TlsMemoryPool&) = delete;
	TlsMemoryPool& operator=(const TlsMemoryPool&) = delete;

public:
	// payloadAlignment 는 2의 거듭제곱이어야 하고 16 이상이어야 한다.
	// Job 처럼 align(64) 인 타입을 담는 풀은 64 를 줘야 한다.
	bool Initialize(const SlabConfig* configs, uint32_t configCount, uint32_t payloadAlignment = 16);
	void Finalize();

	void* Acquire(size_t size);
	void  Release(const void* payload);

	uint32_t GetSlabCount() const { return m_classCount; }
	bool GetSlabStats(uint32_t classIndex, SlabStats& outStats) const;
	void LogStats(const char* poolName) const;

	// 이 풀이 다루는 크기 클래스의 블록 크기. 진단 / 샘플 코드용.
	uint32_t GetClassBlockSize(uint32_t classIndex) const;

	// size 가 어느 클래스로 가는지. classCount 이상이면 OS 우회다. 진단용.
	uint32_t GetClassIndex(size_t size) const;

private:
	struct BlockHeader;
	struct ChunkNode;
	struct Bin;
	struct ThreadCache;
	struct ClassDesc;
	struct GlobalClass;

private:
	// 크기 -> 클래스. 메모리 접근이 없다.
	uint32_t ClassOf(size_t size) const;

	// 블록 헤더에 남길 소유 풀 표식.
	uint8_t OwnerTag() const;

	void* PayloadOf(BlockHeader* header) const;
	BlockHeader* HeaderOf(const void* payload) const;

	// 이 스레드의 캐시를 찾거나 없으면 만든다.
	ThreadCache* GetThreadCache();
	ThreadCache* AttachThreadCache();
	void DetachThreadCache(ThreadCache* cache);
	static void WINAPI ThreadCacheDestructor(PVOID value);

	// 전역에서 batch 개 묶음 하나를 가져와 빈 bin 을 채운다.
	bool RefillBin(ThreadCache* cache, uint32_t classIndex);
	// bin 에서 batch 개를 떼어 전역으로 되돌린다.
	void FlushBin(ThreadCache* cache, uint32_t classIndex, bool flushAll);

	// 스팬 하나를 잡아 클래스의 전역 리스트를 채운다. growLock 을 잡은 채 호출한다.
	bool AllocateSpanLocked(uint32_t classIndex, uint32_t minBlocks, bool isInitial);
	bool RecordSpanLocked(GlobalClass& global, void* span, size_t spanBytes);

	void* AcquireBypass(size_t size);
	void  ReleaseBypass(BlockHeader* header);

private:
	ClassDesc*   m_classes = nullptr;
	GlobalClass* m_globals = nullptr;
	void*        m_globalsRaw = nullptr;   // m_globals 의 정렬 전 원본 포인터

	ThreadCache* m_cacheList = nullptr;    // 살아있는 스레드 캐시 레지스트리
	mutable SRWLOCK m_registryLock = SRWLOCK_INIT;

	// 종료된 스레드들의 지표를 잃지 않도록 여기에 누적한다.
	uint64_t* m_retiredAcquire = nullptr;  // [MAX_CLASS_COUNT]
	uint64_t* m_retiredRelease = nullptr;

	uint32_t m_poolId = UINT32_MAX;
	uint32_t m_generation = 0;
	uint32_t m_classCount = 0;
	uint32_t m_minClassLog = 0;
	size_t   m_minMask = 0;
	uint32_t m_payloadAlignment = 16;
	uint32_t m_headerSize = 0;

	DWORD m_flsIndex = FLS_OUT_OF_INDEXES;
	bool  m_initialized = false;

	// OS 우회 경로 지표
	mutable volatile LONG64 m_bypassAcquire = 0;
	mutable volatile LONG64 m_bypassRelease = 0;
};
