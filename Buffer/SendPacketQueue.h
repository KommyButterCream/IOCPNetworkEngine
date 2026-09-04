#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>
#include <stdint.h>
#include "PreDefine.h"
#include "SendPacketPool.h"

class SlabMemoryPool;

class SendPacketQueue
{
public:
	SendPacketQueue();
	~SendPacketQueue();

	bool Initialize(SendPacketPool* sendPacketPool, SlabMemoryPool* packetMemoryPool, SlabMemoryPool* generalMemoryPool);
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
	SendPacketPool* m_packetPool = nullptr;
	SlabMemoryPool* m_packetMemoryPool = nullptr;
	SlabMemoryPool* m_generalMemoryPool = nullptr;

	int32_t m_count = 0;
};
