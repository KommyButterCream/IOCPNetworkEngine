#pragma once

// Windows.h 파일이 필요한 경우에는
// WIN32_LEAN_AND_MEAN 를 먼저 정의하고
// Windows.h 를 incldue 해야한다.
// 또한, 그 순서는 Windows.h 가 가장 먼저 include 되어야한다.
// Winsock.h 헤더 파일의 선언은 Windows sockets 2.0에 필요한
// Winsock2.h 헤더 파일의 선언과 충돌합니다.
// WIN32_LEAN_AND_MEAN 매크로를 사용하면 
// Winsock.h 가 Windows.h 헤더에 포함되지 않습니다. 
// 

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <Windows.h>

//#ifndef NOMINMAX
//#define NOMINMAX
//#endif

#include <WinSock2.h>
//#include <ws2tcpip.h>
//#include <iphlpapi.h>

#ifdef BUILD_IOCP_ENGINE_DLL
#define IOCP_ENGINE_API __declspec(dllexport)
#else
#define IOCP_ENGINE_API __declspec(dllimport)
#endif

// IOCP Server/Client 를 위한 공용 기능 구현
// Winsock 을 초기화 하고 GQCS 를 위한 스레드풀과 IOCP 핸들을 초기화 한다.
// Server 와 Client 는 IOCPCore 를 상속받아 구현

class ISession;
struct OverlappedEx;
enum class IO_OPERATION;

class IOCP_ENGINE_API IOCPCore
{
public:
	IOCPCore();
	virtual ~IOCPCore();

public:
	bool Start();
	void Stop();

	HANDLE GetIOCPHandle() const noexcept;

protected:
	// 서버, 클라이언트에서 override 필수!
	// GQCS 에 통지 받은 IO 처리
	virtual void HandleCompletion(ULONG_PTR completionKey, LPOVERLAPPED overlapped, DWORD bytesTransferred, BOOL completionStatus) = 0;
	virtual void HandleSocketError(OverlappedEx* overlappedEx, ISession* session, int errorCode, IO_OPERATION ioOperation) = 0;

	bool RegisterSocketToIOCP(ULONG_PTR completionKey, SOCKET socket);
	static constexpr ULONG_PTR TERMINATE_CODE = 0xCAFE;

	// socket Handle
	static void CloseSocketHandle(SOCKET socket);

	void SetIOCPThreadCount(DWORD threadCount);

private:
	// Destory Flag
	LONG m_destroyFlag = 0;

	// Winsock
	bool m_winsockInitialized = false; // Winsock 을 초기화 했는지, 안했는지에 대한 플래그 저장

	// IOCP Handle
	HANDLE m_iocpHandle = INVALID_HANDLE_VALUE; // IOCP 생성 핸들 저장

	// IOCP GQCS Threads
	bool m_iocpWorkerThreadRunning = false;
	LONG m_threadCounter = 0; // ThreadID 카운터
	DWORD m_iocpThreadCount = 0; // IOCP GQCS 스레드 수량
	HANDLE* m_iocpWorkerThreadHandles = nullptr; // IOCP GQCS 스레드 핸들

private:
	// Winsock
	bool InitializeWinsock();
	void FinalizeWinsock();

	// IOCP Handle
	bool InitializeIOCPHandle();
	void FinalizeIOCPHandle();

	// IOCP GQCS Threads
	bool CreateIOCPWorkerthread();
	void DestroyIOCPWorkerthread();
	void RequestIOCPThreadTerminate();
	static unsigned int __stdcall IOCPWorkerThreadProc(LPVOID param);
	void IOCPWorkerThreadLoop();
};



