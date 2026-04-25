#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>
#include <stdint.h>

class SlabMemoryPool;
class ClientSession;

class ClientSessionScheduler
{
public:
	ClientSessionScheduler();
	~ClientSessionScheduler();

public:
	bool Initialize(ClientSession* clientSession, SlabMemoryPool* jobMemoryPool, SlabMemoryPool* packetMemoryPool, SlabMemoryPool* generalMemoryPool);
	void Finalize();

private:
	HANDLE m_thread = nullptr;
	HANDLE m_stopEvent = nullptr;
	volatile LONG m_stopFlag = 0;
	SlabMemoryPool* m_jobMemoryPool = nullptr;
	ClientSession* m_clientSession = nullptr;
	SlabMemoryPool* m_packetMemoryPool = nullptr;
	SlabMemoryPool* m_generalMemoryPool = nullptr;

	static unsigned int __stdcall WorkerThreadProc(LPVOID param);
	void WorkerThreadLoop();
};
