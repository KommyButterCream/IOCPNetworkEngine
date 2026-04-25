#include "SocketOption.h"

#include <WinSock2.h>
#include <MSWSock.h> // for AcceptEX
#include <stdio.h> // for printf_s
#include <mstcpip.h> // for tcp_keepalive

#pragma comment(lib, "ws2_32.lib") // for WinSock2
#pragma comment(lib, "mswsock.lib") // for AcceptEX / ConnectEx

// Reuse address
bool SocketOption::SetResueAddress(SOCKET socket)
{
	BOOL option = TRUE;

	int result = ::setsockopt(socket, SOL_SOCKET, SO_REUSEADDR, (const char*)&option, sizeof(option));

	if (result == SOCKET_ERROR)
	{
		printf_s("setsockopt for SO_REUSEADDR failed with error : %u\n", ::WSAGetLastError());
		return false;
	}

	return true;
}

// TCP No Delay (disable Nagle's Algorithm)
bool SocketOption::SetNoDelay(SOCKET socket)
{
	BOOL option = TRUE;

	int result = ::setsockopt(socket, IPPROTO_TCP, TCP_NODELAY, (const char*)&option, sizeof(option));

	if (result == SOCKET_ERROR)
	{
		printf_s("setsockopt for TCP_NODELAY failed with error : %u\n", ::WSAGetLastError());
		return false;
	}

	return true;
}

// KeepAlive
bool SocketOption::SetKeepAlive(SOCKET socket)
{
	// ���� ���� 2�ð� �� ���� ������ ����ִ��� ù ������ �ϰ� 1�� �������� ��õ� �Ѵ�.
	BOOL option = TRUE;

	int result = ::setsockopt(socket, SOL_SOCKET, SO_KEEPALIVE, (const char*)&option, sizeof(option));

	if (result == SOCKET_ERROR)
	{
		printf_s("setsockopt for SO_KEEPALIVE failed with error : %u\n", ::WSAGetLastError());
		return false;
	}

	return true;
}

bool SocketOption::SetKeepAliveEx(SOCKET socket, DWORD keepAliveTime_ms, DWORD keepAliveInterval_ms)
{
	BOOL option = TRUE;
	if (::setsockopt(socket, SOL_SOCKET, SO_KEEPALIVE, (const char*)&option, sizeof(option)) == SOCKET_ERROR)
	{
		printf_s("SO_KEEPALIVE failed: %u\n", WSAGetLastError());
		return false;
	}

	tcp_keepalive alive;
	alive.onoff = 1;
	alive.keepalivetime = keepAliveTime_ms;
	alive.keepaliveinterval = keepAliveInterval_ms;

	DWORD bytesReturned = 0;
	if (WSAIoctl(socket, SIO_KEEPALIVE_VALS,
		&alive, sizeof(alive),
		nullptr, 0, &bytesReturned, nullptr, nullptr) == SOCKET_ERROR)
	{
		printf_s("SIO_KEEPALIVE_VALS failed: %u\n", WSAGetLastError());
		return false;
	}

	return true;
}

// Receive Buffer Size
bool SocketOption::SetReceiveBufferSize(SOCKET socket, int bufferSize)
{
	int result = ::setsockopt(socket, SOL_SOCKET, SO_RCVBUF, (const char*)&bufferSize, sizeof(bufferSize));

	if (result == SOCKET_ERROR)
	{
		printf_s("setsockopt for SO_RCVBUF failed with error : %u\n", ::WSAGetLastError());
		return false;
	}

	return true;
}

// Send Buffer Size
bool SocketOption::SetSendBufferSize(SOCKET socket, int bufferSize)
{
	int result = ::setsockopt(socket, SOL_SOCKET, SO_SNDBUF, (const char*)&bufferSize, sizeof(bufferSize));

	if (result == SOCKET_ERROR)
	{
		printf_s("setsockopt for SO_SNDBUF failed with error : %u\n", ::WSAGetLastError());
		return false;
	}

	return true;
}

// Linger
bool SocketOption::SetLinger(SOCKET socket, bool enable, int lingerTime)
{
	struct linger lingerOption;
	lingerOption.l_onoff = enable ? 1 : 0;
	lingerOption.l_linger = lingerTime;

	int result = ::setsockopt(socket, SOL_SOCKET, SO_LINGER, (const char*)&lingerOption, sizeof(lingerOption));

	if (result == SOCKET_ERROR)
	{
		printf_s("setsockopt for SO_LINGER failed with error : %u\n", ::WSAGetLastError());
		return false;
	}

	return true;
}

// Update Accept Context
bool SocketOption::SetAcceptContext(SOCKET clientSocket, SOCKET listenSocket)
{
	int result = ::setsockopt(clientSocket, SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT, (const char*)&listenSocket, sizeof(listenSocket));

	if (result == SOCKET_ERROR)
	{
		printf_s("setsockopt for SO_UPDATE_ACCEPT_CONTEXT failed with error : %u\n", ::WSAGetLastError());
		return false;
	}

	return true;
}

// Update Connect Context
bool SocketOption::SetClientContext(SOCKET clientSocket)
{
	int result = ::setsockopt(clientSocket, SOL_SOCKET, SO_UPDATE_CONNECT_CONTEXT, nullptr, 0);

	if (result == SOCKET_ERROR)
	{
		printf_s("setsockopt for SO_UPDATE_CONNECT_CONTEXT failed with error : %u\n", ::WSAGetLastError());
		return false;
	}

	return true;
}

