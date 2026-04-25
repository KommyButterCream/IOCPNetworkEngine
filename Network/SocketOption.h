#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <Windows.h>

typedef unsigned __int64 UINT_PTR, * PUINT_PTR;
typedef UINT_PTR SOCKET;

class SocketOption
{
public:
	// Reuse address
	static bool SetResueAddress(SOCKET socket);

	// TCP No Delay (disable Nagle's Algorithm)
	static bool SetNoDelay(SOCKET socket);

	// KeepAlive
	static bool SetKeepAlive(SOCKET socket);
	static bool SetKeepAliveEx(SOCKET socket, DWORD keepAliveTime_ms, DWORD keepAliveInterval_ms);

	// Receive Buffer Size
	static bool SetReceiveBufferSize(SOCKET socket, int bufferSize);

	// Send Buffer Size
	static bool SetSendBufferSize(SOCKET socket, int bufferSize);

	// Linger
	static bool SetLinger(SOCKET socket, bool enable, int lingerTime = 0);

	// Update Accept Context
	static bool SetAcceptContext(SOCKET clientSocket, SOCKET listenSocket);

	// Update Connect Context
	static bool SetClientContext(SOCKET clientSocket);
};

