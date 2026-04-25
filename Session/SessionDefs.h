#pragma once

#include <stdint.h>

enum class IO_OPERATION
{
	INVALID = 0,
	ACCEPT,
	CONNECT,
	RECV,
	SEND
};

enum class SESSION_ROLE
{
	NONE,
	CLIENT,
	SERVER,
	ACCEPT,
};


static constexpr uint32_t DUMMY_SESSION_ID = UINT32_MAX - 1;
static constexpr uint32_t INVALID_SESSION_ID = UINT32_MAX;