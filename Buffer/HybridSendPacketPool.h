#pragma once

#include <stdint.h>

// Session 마다 SendPacketBuffer 를 고정 크기로 가지고 있으면 Session 의 크기가 너무 커지고
// SendPacketPool 을 모든 Session 이 공유해서 사용하면 Pool 에서 PacketBuffer 를 Get/Release 할 때 마다 Lock 경합이 발생되어
// 세션의 수량이 많을 수록 처리량이 낮아지게 될 것이다.
// 그러므로 SendPacketPool 을 Pool 로 가지는 HybridSendPacketPool 을 작성 및 생성하여
// SessionID % HYBRID_POOL_COUNT 의 SendPacketPool 을 공유해서 사용하도록 한다.
// 하나의 SendPacketPool 을 여러 Session 이 공유해서 사용.
// HYBRID_POOL_COUNT 만큼의 Get/Release 요청이 Lock 경합 없이 동시 처리 가능!

// HybridSendPacketPool m_nTotalBlockCount == 4096, HYBRID_POOL_COUNT == 8 일 때
// nPerPool = m_nTotalBlockCount(4096) / HYBRID_POOL_COUNT(8) = 512
//  - SendPacketPool[0] (SendPacketBuffer 512개 공유) : SessionID(0), SessionID(8), SessionID(16) ...
//  - SendPacketPool[1] (SendPacketBuffer 512개 공유) : SessionID(1), SessionID(9), SessionID(17) ...
//  - SendPacketPool[2] (SendPacketBuffer 512개 공유) : SessionID(2), SessionID(10), SessionID(18) ...
//  - ...
//  - SendPacketPool[7] (SendPacketBuffer 512개 공유) : SessionID(7), SessionID(15), SessionID(23) ...
//  

class SendPacketPool;

class HybridSendPacketPool
{
public:
	HybridSendPacketPool();
	~HybridSendPacketPool();

	bool Initialize(uint32_t totalBlockCount, uint32_t hybridPoolCount = 0);
	void Finalize();

	SendPacketPool* GetPool(const uint32_t sessionId);

private:
	uint32_t m_totalBlockCount = 0;
	uint32_t m_hybridPoolCount = 0;

	SendPacketPool** m_hybridPools = nullptr;
};
