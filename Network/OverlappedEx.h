#pragma once

#include <WinSock2.h>

#include "../Session/SessionDefs.h"

struct OverlappedEx
{
	WSAOVERLAPPED wsaOverlapped = {};
	WSABUF wsaBuffer = {};
	IO_OPERATION operation = IO_OPERATION::INVALID;
	uint32_t sessionId = INVALID_SESSION_ID;
};
