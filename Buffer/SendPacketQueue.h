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

private:
	bool IsEmpty() const;

private:
	alignas(64) int32_t m_head = 0;
	alignas(64) int32_t m_tail = 0;
	alignas(64) SRWLOCK m_srwLock = SRWLOCK_INIT;

	SendPacketBuffer** m_queue = nullptr;
	SendPacketPool* m_packetPool = nullptr;
	SlabMemoryPool* m_packetMemoryPool = nullptr;
	SlabMemoryPool* m_generalMemoryPool = nullptr;

	int32_t m_count = 0;
};
