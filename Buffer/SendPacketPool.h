#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>
#include <stdint.h>
#include "PreDefine.h"

using SendPacketReleaseFunc = void (*)(const void* packetData, void* context);

struct alignas(64) SendPacketBuffer : public SLIST_ENTRY
{
	const char* packetData = nullptr;    // 각 블록 버퍼 크기
	uint32_t packetSize = 0;
	SendPacketReleaseFunc releaseFunc = nullptr;
	void* releaseContext = nullptr;

	inline void Reset() noexcept
	{
		packetData = nullptr;
		packetSize = 0;
		releaseFunc = nullptr;
		releaseContext = nullptr;
	}
};

class SendPacketPool
{
public:
	SendPacketPool();
	~SendPacketPool();

	bool Initialize(uint32_t blockCount = 512);
	void Finalize();

	SendPacketBuffer* Acquire();
	void Release(SendPacketBuffer* packetBuffer);

private:
	alignas(64) SLIST_HEADER m_sListHead;
	alignas(64) uint64_t m_acqCount = 0; // debug
	alignas(64) uint64_t m_relCount = 0; // debug

	SendPacketBuffer* m_bufferPool = nullptr;        // 연속 메모리
	uint32_t m_blockCount = 0;
};
