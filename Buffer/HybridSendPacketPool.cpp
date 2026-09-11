#include "HybridSendPacketPool.h"
#include "SendPacketPool.h"

#include "../../Core/Util/Logger.h"

using namespace Core::Util;

HybridSendPacketPool::HybridSendPacketPool()
{
}

HybridSendPacketPool::~HybridSendPacketPool()
{
	Finalize();
}

bool HybridSendPacketPool::Initialize(uint32_t totalBlockCount, uint32_t hybridPoolCount)
{
	Finalize();

	m_totalBlockCount = totalBlockCount;
	m_hybridPoolCount = hybridPoolCount;

	if (m_totalBlockCount == 0)
	{
		return false;
	}

	if (m_hybridPoolCount == 0)
	{
		uint32_t cpuProcessorCount = ::GetMaximumProcessorCount(ALL_PROCESSOR_GROUPS);
		if (cpuProcessorCount == 0)
		{
			cpuProcessorCount = ::GetActiveProcessorCount(ALL_PROCESSOR_GROUPS);
		}
		if (cpuProcessorCount == 0)
		{
			cpuProcessorCount = 1;
		}

		m_hybridPoolCount = static_cast<uint32_t>(cpuProcessorCount);
	}

	if (m_hybridPoolCount == 0)
	{
		Finalize();
		return false;
	}

	const uint32_t perPoolCount = m_totalBlockCount / m_hybridPoolCount;
	if (perPoolCount == 0)
	{
		Finalize();
		return false;
	}

	// 나머지는 버려진다. 총량을 그대로 쓸 수 없다는 사실을 남긴다.
	const uint32_t discarded = m_totalBlockCount - (perPoolCount * m_hybridPoolCount);
	if (discarded > 0)
	{
		LOGI("send packet pool : %u blocks over %u shards leaves %u unused (%u per shard)",
			m_totalBlockCount, m_hybridPoolCount, discarded, perPoolCount);
	}

	m_hybridPools = new SendPacketPool * [m_hybridPoolCount] {};
	if (!m_hybridPools)
	{
		Finalize();
		return false;
	}

	for (uint32_t i = 0; i < m_hybridPoolCount; i++)
	{
		m_hybridPools[i] = new SendPacketPool();
		if (!m_hybridPools[i])
		{
			Finalize();
			return false;
		}

		if (!m_hybridPools[i]->Initialize(perPoolCount))
		{
			Finalize();
			return false;
		}
	}

	return true;
}

void HybridSendPacketPool::Finalize()
{
	if (m_hybridPools)
	{
		for (uint32_t i = 0; i < m_hybridPoolCount; i++)
		{
			if (!m_hybridPools[i])
				continue;

			m_hybridPools[i]->Finalize();
			delete m_hybridPools[i];
			m_hybridPools[i] = nullptr;
		}
	}

	delete[] m_hybridPools;
	m_hybridPools = nullptr;

	m_totalBlockCount = 0;
	m_hybridPoolCount = 0;
}

SendPacketPool* HybridSendPacketPool::GetPool(const uint32_t sessionId)
{
	if (!m_hybridPools || m_totalBlockCount == 0 || m_hybridPoolCount == 0)
		return nullptr;

	// SessionID 에 따라 사용할 SendPacketPool 을 부여한다.
	return m_hybridPools[sessionId % m_hybridPoolCount];
}
