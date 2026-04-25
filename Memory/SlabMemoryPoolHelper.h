#pragma once

#include "SlabMemoryPool.h"

#include "../Modules/Core/Util/Logger.h"

#include "../Job/Job.h"
#include "../Protocol/PacketHeader.h"
#include "../Protocol/PacketID.h"

#include <new>
#include <utility>

#include <stdio.h> // for printf_s

using namespace Core::Util;

namespace MEMORY_POOL
{
	// Utility helpers for Packet objects

	inline void* CreatePacket(SlabMemoryPool& pool, size_t size)
	{
		// 오브젝트 생성
		// 메모리 풀에서 크기에 맞는 메모리를 찾아서 반환

		void* memory = pool.Acquire(size);

		Logger::Log(LogLevel::LOG_INFO, "[%s] Create Packet : %p", __FUNCTION__, memory);

		if (!memory)
			return nullptr;

		return memory;
	}

	inline void ReleasePacket(SlabMemoryPool& packetPool, SlabMemoryPool& generalPool, const void* memory)
	{
		// 오브젝트 해제
		// 사용이 끝난 메모리를 메모리풀에 반환
		if (!memory)
			return;

		Logger::Log(LogLevel::LOG_INFO, "[%s] Release Packet : %p", __FUNCTION__, memory);


		//PACKET_HEADER* header = reinterpret_cast<PACKET_HEADER*>(memory);

		//switch (header->packetId)
		//{
		//case ToPacketID(PACKET_ID::SERVICE_BEGIN):
		//{
		//	auto* userPacket = reinterpret_cast<INFERENCE_IMAGE_PACKET_REQUEST*>(memory);
		//	if (userPacket->pImageBuffer)
		//		generalPool.Release(userPacket->pImageBuffer);
		//	break;
		//}
		//}

		// 사용이 끝난 메모리를 메모리풀에 반환
		packetPool.Release(memory);
	}

	// Utility helpers for Job objects

	inline Job* CreateJob(SlabMemoryPool& pool)
	{
		// Job 메모리 버퍼를 버퍼풀로부터 반환

		void* memory = pool.Acquire(sizeof(Job));

		if (!memory)
		{
			Logger::Log(LogLevel::LOG_ERROR, "[%s] Create Job Failed", __FUNCTION__);

			return nullptr;
		}

		Logger::Log(LogLevel::LOG_INFO, "[%s] Create Job : %p", __FUNCTION__, memory);

		return static_cast<Job*>(memory);
	}

	inline void ReleaseJob(SlabMemoryPool& pool, Job* job)
	{
		// 사용이 끝난 Job 메모리 버퍼를 버퍼풀에 반환
		if (!job)
		{
			Logger::Log(LogLevel::LOG_ERROR, "[%s] Invalid Job", __FUNCTION__);

			return;
		}

		Logger::Log(LogLevel::LOG_INFO, "[%s] Release Job : %p", __FUNCTION__, job);

		// 사용이 끝난 메모리를 메모리풀에 반환
		pool.Release(static_cast<const void*>(job));
	}
}
