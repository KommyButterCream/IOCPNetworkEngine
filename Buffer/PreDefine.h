#pragma once
#include <stdint.h>

constexpr static uint32_t MEMORY_SIZE_1K = 1024;
constexpr static uint32_t MEMORY_SIZE_2K = 1024 * 2;
constexpr static uint32_t MEMORY_SIZE_4K = 1024 * 4;
constexpr static uint32_t MEMORY_SIZE_8K = 1024 * 8;
constexpr static uint32_t MEMORY_SIZE_16K = 1024 * 16;
constexpr static uint32_t MEMORY_SIZE_32K = 1024 * 32;
constexpr static uint32_t MEMORY_SIZE_64K = 1024 * 64;
constexpr static uint32_t MEMORY_SIZE_128K = 1024 * 128;
constexpr static uint32_t MEMORY_SIZE_256K = 1024 * 256;
constexpr static uint32_t MEMORY_SIZE_512K = 1024 * 512;
constexpr static uint32_t MEMORY_SIZE_1024K = 1024 * 1024;

constexpr static uint32_t MEMORY_SIZE_1MB = MEMORY_SIZE_1024K;
constexpr static uint32_t MEMORY_SIZE_2MB = MEMORY_SIZE_1024K * 2;
constexpr static uint32_t MEMORY_SIZE_4MB = MEMORY_SIZE_1024K * 4;
constexpr static uint32_t MEMORY_SIZE_8MB = MEMORY_SIZE_1024K * 8;
constexpr static uint32_t MEMORY_SIZE_16MB = MEMORY_SIZE_1024K * 16;
constexpr static uint32_t MEMORY_SIZE_32MB = MEMORY_SIZE_1024K * 32;
constexpr static uint32_t MEMORY_SIZE_64MB = MEMORY_SIZE_1024K * 64;
constexpr static uint32_t MEMORY_SIZE_128MB = MEMORY_SIZE_1024K * 128;
constexpr static uint32_t MEMORY_SIZE_256MB = MEMORY_SIZE_1024K * 256;
constexpr static uint32_t MEMORY_SIZE_512MB = MEMORY_SIZE_1024K * 512;
constexpr static uint32_t MEMORY_SIZE_1024MB = MEMORY_SIZE_1024K * 1024;


constexpr static uint32_t BLOCK_COUNT_1K = 1024;
constexpr static uint32_t BLOCK_COUNT_2K = 1024 * 2;
constexpr static uint32_t BLOCK_COUNT_4K = 1024 * 4;
constexpr static uint32_t BLOCK_COUNT_8K = 1024 * 8;
constexpr static uint32_t BLOCK_COUNT_16K = 1024 * 16;
constexpr static uint32_t BLOCK_COUNT_32K = 1024 * 32;
constexpr static uint32_t BLOCK_COUNT_64K = 1024 * 64;


constexpr static uint32_t RECV_PACKET_BUFFER_SIZE = MEMORY_SIZE_1MB;
constexpr static uint32_t SEND_PACKET_QUEUE_SIZE = BLOCK_COUNT_4K;
constexpr static uint32_t HYBRID_SEND_PACKET_POOL_SIZE = BLOCK_COUNT_64K;
constexpr static uint32_t INFERENCE_CHUNK_SIZE = MEMORY_SIZE_4K;