#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>
#include <stdint.h>
#include "PreDefine.h"
#include "SendPacketPool.h"

#include "../Memory/EngineMemoryPoolFwd.h"

class SendPacketQueue
{
public:
	SendPacketQueue();
	~SendPacketQueue();

	bool Initialize(SendPacketPool* sendPacketPool, EngineMemoryPool* packetMemoryPool, EngineMemoryPool* generalMemoryPool, uint32_t depth);
	void Finalize();

	bool Enqueue(void** packetData, uint32_t packetSize);
	bool EnqueueShared(const void* packetData, uint32_t packetSize, SendPacketReleaseFunc releaseFunc, void* releaseContext);
	bool Dequeue(SendPacketBuffer*& outBlock);

	void Reset();

	// 락을 획득해서 확인한다. 락을 보유하지 않은 외부 경로에서 사용한다.
	bool IsEmpty() const;

private:
	// 락을 보유한 상태에서만 호출한다.
	// SRWLOCK 은 재귀 획득이 불가하므로 내부 경로는 반드시 이 버전을 사용해야 한다.
	bool IsEmptyLocked() const;

private:
	alignas(64) int32_t m_head = 0;
	alignas(64) int32_t m_tail = 0;
	alignas(64) mutable SRWLOCK m_srwLock = SRWLOCK_INIT;

	SendPacketBuffer** m_queue = nullptr;

	// 예전에는 컴파일 타임 상수였다. 역할마다 적정 깊이가 달라 런타임으로 옮겼다.
	int32_t m_capacity = 0;
	int32_t m_capacityMask = 0;
	SendPacketPool* m_packetPool = nullptr;
	EngineMemoryPool* m_packetMemoryPool = nullptr;
	EngineMemoryPool* m_generalMemoryPool = nullptr;

	int32_t m_count = 0;
};
