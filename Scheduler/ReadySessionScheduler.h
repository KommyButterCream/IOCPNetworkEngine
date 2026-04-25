#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>
#include <stdint.h>

class ReadySessionQueue;
class SlabMemoryPool;

class ReadySessionScheduler
{
public:
	ReadySessionScheduler();
	~ReadySessionScheduler();

public:
	bool Initialize(uint32_t workerCount, ReadySessionQueue* readySessionQueue, SlabMemoryPool* jobMemoryPool, SlabMemoryPool* packetMemoryPool, SlabMemoryPool* generalMemoryPool);
	void Finalize();

private:
	HANDLE* m_threads = nullptr;
	uint32_t m_workerCount = 0;
	volatile LONG m_stopFlag = 0;
	ReadySessionQueue* m_readySessionQueue = nullptr;
	SlabMemoryPool* m_jobMemoryPool = nullptr;
	SlabMemoryPool* m_packetMemoryPool = nullptr;
	SlabMemoryPool* m_generalMemoryPool = nullptr;

	static unsigned int __stdcall WorkerThreadProc(LPVOID param);
	void WorkerThreadLoop();
};
