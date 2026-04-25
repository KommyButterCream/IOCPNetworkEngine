#pragma once

#include <stdint.h>

#include "PreDefine.h"

#include "../Protocol/PacketHeader.h"

class SlabMemoryPool;

class RecvPacketBuffer
{
public:
	RecvPacketBuffer();
	~RecvPacketBuffer();

	RecvPacketBuffer(const RecvPacketBuffer&) = delete;
	RecvPacketBuffer& operator=(const RecvPacketBuffer&) = delete;
	RecvPacketBuffer(RecvPacketBuffer&&) = delete;
	RecvPacketBuffer& operator=(RecvPacketBuffer&&) = delete;

private:
	SlabMemoryPool* m_packetMemoryPool = nullptr;

	char* m_buffer = nullptr;
	uint32_t m_writePos = 0;
	uint32_t m_readPos = 0;
	uint32_t m_storedSize = 0;

public:
	bool Initialize(SlabMemoryPool* packetMemoryPool);
	void Finalize();
	void Reset();

	// WSARecv용
	char* GetWriteablePtr();
	uint32_t   GetWriteableSize() const;

	// 수신 완료 후 처리
	bool CommitWrite(const uint32_t bytesReceived);

	// 패킷 추출
	bool ReadPacket(char*& outBuffer, uint32_t& outSize, uint16_t& outPacketId);

	// 상태 확인
	uint32_t GetStoredSize() const { return m_storedSize; }
	bool IsFull() const { return m_storedSize == RECV_PACKET_BUFFER_SIZE; }
	bool IsEmpty() const { return m_storedSize == 0; }

private:
	bool PeekHeader(PACKET_HEADER& header);
};

