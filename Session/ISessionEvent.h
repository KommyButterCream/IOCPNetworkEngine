#pragma once

#include <stdint.h>

#ifdef BUILD_IOCP_ENGINE_DLL
#define IOCP_ENGINE_API __declspec(dllexport)
#else
#define IOCP_ENGINE_API __declspec(dllimport)
#endif

class ISession;

class IOCP_ENGINE_API ISessionEvent
{
public:
	virtual void OnDisconnectRequest(ISession* session) = 0;
	virtual ~ISessionEvent() {}
};

typedef void (*CloseSocketFunc)(uintptr_t);