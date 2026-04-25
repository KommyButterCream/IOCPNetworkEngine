#include "RecvPacketBuffer.h"

#include "PreDefine.h"

#include "../Memory/SlabMemoryPool.h"
#include "../Memory/SlabMemoryPoolHelper.h"

#include <memory.h>
#include <assert.h> // for assert
#include <malloc.h> // for _aligned_malloc, _aligned_free

RecvPacketBuffer::RecvPacketBuffer()
{
}

bool RecvPacketBuffer::Initialize(SlabMemoryPool* packetMemoryPool)
{
	static_assert((RECV_PACKET_BUFFER_SIZE & (RECV_PACKET_BUFFER_SIZE - 1)) == 0, "Buffer size must be power of 2");

	if (packetMemoryPool == nullptr)
	{
		return false;
	}

	Finalize();

	m_packetMemoryPool = packetMemoryPool;
	m_buffer = static_cast<char*>(_aligned_malloc(RECV_PACKET_BUFFER_SIZE, 64));
	if (!m_buffer)
	{
		Finalize();
		return false;
	}

	Reset();
	return true;
}

RecvPacketBuffer::~RecvPacketBuffer()
{
	Finalize();
}

void RecvPacketBuffer::Finalize()
{
	m_packetMemoryPool = nullptr;

	m_writePos = 0;
	m_readPos = 0;
	m_storedSize = 0;

	if (m_buffer)
	{
		_aligned_free(m_buffer);
		m_buffer = nullptr;
	}
}

void RecvPacketBuffer::Reset()
{
	m_writePos = 0;
	m_readPos = 0;
	m_storedSize = 0;
}

char* RecvPacketBuffer::GetWriteablePtr()
{
	if (!m_buffer)
	{
		return nullptr;
	}

	return m_buffer + m_writePos;
}

uint32_t RecvPacketBuffer::GetWriteableSize() const
{
	if (!m_buffer)
		return 0;

	if (IsFull())
		return 0;

	if (m_writePos >= m_readPos)
	{
		const uint32_t spaceToEnd = RECV_PACKET_BUFFER_SIZE - m_writePos;
		const uint32_t totalSpace = RECV_PACKET_BUFFER_SIZE - m_storedSize;

		return spaceToEnd < totalSpace ? spaceToEnd : totalSpace;
	}
	else
		return m_readPos - m_writePos;
}

bool RecvPacketBuffer::CommitWrite(const uint32_t bytesReceived)
{
	if (!m_buffer || bytesReceived == 0)
		return false;

	const uint32_t writeableSize = GetWriteableSize();
	if (bytesReceived > writeableSize)
	{
		// 버퍼 overflow
		__debugbreak();
		return false;
	}

	m_writePos = (m_writePos + bytesReceived) & (RECV_PACKET_BUFFER_SIZE - 1);
	m_storedSize += bytesReceived;

	return true;
}

bool RecvPacketBuffer::PeekHeader(PACKET_HEADER& header)
{
	if (!m_buffer || IsEmpty())
		return false;

	if (m_storedSize < sizeof(PACKET_HEADER))
		return false;

	uint32_t remainFirst = RECV_PACKET_BUFFER_SIZE - m_readPos;
	if (remainFirst >= sizeof(PACKET_HEADER)) [[likely]]
	{
		memcpy(&header, m_buffer + m_readPos, sizeof(PACKET_HEADER));
	}
	else [[unlikely]]
	{
		// 헤더 split
		memcpy(&header, m_buffer + m_readPos, remainFirst);
		memcpy(reinterpret_cast<char*>(&header) + remainFirst, m_buffer, sizeof(PACKET_HEADER) - remainFirst);
	}

	return true;
}

bool RecvPacketBuffer::ReadPacket(char*& outBuffer, uint32_t& outSize, uint16_t& outPacketId)
{
	outSize = 0;
	outPacketId = 0;
	outBuffer = nullptr;

	PACKET_HEADER header{};
	if (!PeekHeader(header))
		return false;

	if (header.packetSize < sizeof(PACKET_HEADER) || header.packetSize > RECV_PACKET_BUFFER_SIZE)
		return false;

	if (m_storedSize < header.packetSize)
		return false; // 아직 패킷 전체 수신 안됨

	if (!m_packetMemoryPool)
		return false;

	// 패킷 메모리 버퍼 풀로 부터
	// RecvPacketBuffer 의 m_buffer 에 저장된 패킷 데이터를 카피한다.

	char* packetMemory = reinterpret_cast<char*>(MEMORY_POOL::CreatePacket(*m_packetMemoryPool, header.packetSize));

	//char* packetMemory = reinterpret_cast<char*>(m_packetMemoryPool->Acquire(header.packetSize));
	if (!packetMemory)
	{
		__debugbreak();
		return false;
	}

	uint32_t remainFirst = RECV_PACKET_BUFFER_SIZE - m_readPos;
	if (header.packetSize <= remainFirst) [[likely]]
	{
		memcpy(packetMemory, m_buffer + m_readPos, header.packetSize);
	}
	else [[unlikely]]
	{
		// payload split
		memcpy(packetMemory, m_buffer + m_readPos, remainFirst);
		memcpy(packetMemory + remainFirst, m_buffer, header.packetSize - remainFirst);
	}

	outBuffer = packetMemory;
	outSize = header.packetSize;
	outPacketId = header.packetId;

	m_readPos = (m_readPos + header.packetSize) & (RECV_PACKET_BUFFER_SIZE - 1);
	m_storedSize -= header.packetSize;

	return true;
}

