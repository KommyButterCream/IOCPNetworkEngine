#include "HybridSendPacketPool.h"
#include "SendPacketPool.h"

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
	for (uint32_t i = 0; i < m_hybridPoolCount; i++)
	{
		if (m_hybridPools && m_hybridPools[i])
		{
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

	// SessionID �� ���� SendPacketPool �� �ο��Ѵ�.
	return m_hybridPools[sessionId % m_hybridPoolCount];
}
