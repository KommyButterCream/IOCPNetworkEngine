#pragma once

#ifdef BUILD_IOCP_ENGINE_DLL
#define IOCP_ENGINE_API __declspec(dllexport)
#else
#define IOCP_ENGINE_API __declspec(dllimport)
#endif

class IOCP_ENGINE_API SessionContext
{
public:
	virtual ~SessionContext() = default;
};
