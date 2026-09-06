#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>

#include "BlockLayout.h"
#include "SizeBin.h"

#ifndef IOCP_ENGINE_API
#ifdef BUILD_IOCP_ENGINE_DLL
#define IOCP_ENGINE_API __declspec(dllexport)
#else
#define IOCP_ENGINE_API __declspec(dllimport)
#endif
#endif

// 블록의 실체를 소유하는 계층.
//
//   - 세그먼트(VirtualAlloc)를 잡아 stride 간격으로 자른다
//   - 자른 블록을 batch 개씩 묶음(chunk)으로 엮어 빈별 SLIST 에 보관한다
//   - 스레드 캐시가 묶음 단위로 가져가고 되돌려 놓는다
//
// 이 클래스는 스레드 캐시를 모른다. 의존은 캐시 -> 풀 한 방향뿐이다.
//
// 동기화
//   PopChunk / PushChunk : Interlocked SLIST. 락 없음
//   세그먼트 확장         : SRWLOCK. 정상 운영 중에는 호출되지 않음
//
// 묶음(chunk)이 어떻게 표현되는지는 이 클래스의 구현 사항이다.
// 바깥에는 ChunkRef(머리 포인터 + 개수)만 노출한다.

namespace MemoryPoolDetail
{
	class IOCP_ENGINE_API GlobalBlockPool
	{
	public:
		// head 가 nullptr 이면 실패
		struct ChunkRef
		{
			BlockHeader* head = nullptr;
			uint32_t count = 0;
		};

		// 빈 하나의 운영 지표
		struct BinStats
		{
			uint32_t blocksCreated = 0;   // 세그먼트에서 실제로 만들어낸 블록 수
			uint32_t blocksInPool = 0;    // 지금 SLIST 에 남아 있는 블록 수
			uint32_t peakOutOfPool = 0;   // 동시에 밖에 나가 있던 블록 수의 상한
			uint32_t growthCount = 0;     // 런타임 확장 횟수
			uint32_t failCount = 0;       // 세그먼트 확보 실패 횟수
			uint32_t segmentCount = 0;
			uint64_t committedBytes = 0;   // 이 빈이 OS 에서 잡은 총 바이트
		};

	public:
		GlobalBlockPool() = default;
		~GlobalBlockPool();

		GlobalBlockPool(const GlobalBlockPool&) = delete;
		GlobalBlockPool& operator=(const GlobalBlockPool&) = delete;

		bool Initialize(const BinTable& table, uint8_t ownerTag);
		void Finalize();

		// 설정에 적힌 만큼 미리 만들어 둔다. 세그먼트 최소 크기 때문에
		// 요청보다 많이 만들어질 수 있다.
		bool Preallocate(uint32_t bin, uint32_t blocks);

		// 묶음 하나를 꺼낸다. 재고가 없으면 세그먼트를 늘려서라도 채운다.
		ChunkRef PopChunk(uint32_t bin);

		// 묶음 하나를 되돌린다. head 는 count 개짜리 사슬의 머리여야 한다.
		void PushChunk(uint32_t bin, BlockHeader* head, uint32_t count);

		bool GetStats(uint32_t bin, BinStats& out) const;

		const BinTable& Table() const { return *m_table; }

	private:
		struct Bin;   // .cpp 에 정의

		// growLock 을 잡은 채 호출한다.
		bool GrowLocked(uint32_t bin, uint32_t minBlocks, bool isInitial);
		bool RecordSegmentLocked(Bin& target, void* segment, size_t bytes);

		void CarveAndPush(Bin& target, uint32_t bin, void* segment, uint32_t blocks);
		void NotePopped(Bin& target, uint32_t count);

	private:
		const BinTable* m_table = nullptr;
		Bin*  m_bins = nullptr;
		void* m_binsRaw = nullptr;      // m_bins 의 정렬 전 원본 포인터
		uint8_t m_ownerTag = POOL_ID_NONE;
		bool m_initialized = false;
	};
}
