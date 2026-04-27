#include "IOCPCore.h"

#include <process.h> // for _beginthread, _endthread
#include <stdio.h> // for printf_s

#include "../../Core/Util/Logger.h"

#pragma comment(lib, "ws2_32.lib") // for WinSock2

using namespace Core::Util;

IOCPCore::IOCPCore()
{
}

IOCPCore::~IOCPCore()
{
	Stop();
}

bool IOCPCore::Start()
{
	if (!InitializeWinsock())
	{
		return false;
	}

	if (!InitializeIOCPHandle())
	{
		return false;
	}

	if (!CreateIOCPWorkerthread())
	{
		return false;
	}

	return true;
}

void IOCPCore::Stop()
{
	if (::InterlockedCompareExchange(&m_destroyFlag, 1, 0) == 1)
		return;

	printf_s("%s\n", __FUNCTION__);

	RequestIOCPThreadTerminate();
	DestroyIOCPWorkerthread();
	FinalizeIOCPHandle();
	FinalizeWinsock();
}

HANDLE IOCPCore::GetIOCPHandle() const noexcept
{
	return m_iocpHandle;
}

bool IOCPCore::InitializeWinsock()
{
	WSADATA wsaData;
	int result(-1);
	constexpr int sucesss(0);

	::ZeroMemory(&wsaData, sizeof(WSADATA));

	// WS2_32.dll 사용을 시작하기 위해 호출
	result = ::WSAStartup(MAKEWORD(2, 2), &wsaData);
	if (result != sucesss)
	{
		//Log::log(LogLevel::LOG_ERROR, "[%s] WSAStartup failed\n", __FUNCTION__);

		return false;
	}

	m_winsockInitialized = true;

	//Log::log(LogLevel::LOG_INFO, "[%s] WSAStartup success\n", __FUNCTION__);

	return true;
}

void IOCPCore::FinalizeWinsock()
{
	if (m_winsockInitialized)
	{
		Logger::Log(LogLevel::LOG_INFO, "[%s] IOCP WinSocket Cleanup", __FUNCTION__);

		// WS2_32.dll 사용 해제를 위해 호출
		if (::WSACleanup() == SOCKET_ERROR)
		{
			//Log::log(LogLevel::LOG_ERROR, "[%s] WSACleanup Failed - ErroCode : %d\n", __FUNCTION__, WSAGetLastError());
		}

		m_winsockInitialized = false;

		//Log::log(LogLevel::LOG_INFO, "[%s] WSACleanup success\n", __FUNCTION__);
	}
}

bool IOCPCore::InitializeIOCPHandle()
{
	m_iocpHandle = ::CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0);
	if (m_iocpHandle == INVALID_HANDLE_VALUE)
	{
		//Log::log(LogLevel::LOG_ERROR, "[%s] CreateIoCompletionPort failed - ErroCode : %d\n", __FUNCTION__, WSAGetLastError());

		return false;
	}

	//Log::log(LogLevel::LOG_INFO, "[%s] CreateIoCompletionPort success\n", __FUNCTION__);

	return true;
}

void IOCPCore::FinalizeIOCPHandle()
{
	if (m_iocpHandle != INVALID_HANDLE_VALUE)
	{
		Logger::Log(LogLevel::LOG_INFO, "[%s] IOCP Handle Close", __FUNCTION__);

		::CloseHandle(m_iocpHandle);
		m_iocpHandle = INVALID_HANDLE_VALUE;
	}
}

void IOCPCore::CloseSocketHandle(SOCKET socket)
{
	// Listen, Client 세션의 소켓 해제
	// 소켓 생성을 Server/Client 가 했으므로
	// 닫아주는 역할도 Server/Client 가 해주어야 한다. 세션이 아니다.

	if (socket != INVALID_SOCKET)
	{
		if (::closesocket(socket) == SOCKET_ERROR)
		{
			const int nError = ::WSAGetLastError();

			Logger::Log(LogLevel::LOG_ERROR, "[%s][socketID : %d] closesocket 실패(ERROR CODE : %d)", __FUNCTION__, static_cast<int>(socket), nError);
		}

		Logger::Log(LogLevel::LOG_INFO, "[%s][socketID : %d] closesocket 완료", __FUNCTION__, static_cast<int>(socket));
	}
}

void IOCPCore::SetIOCPThreadCount(DWORD threadCount)
{
	m_iocpThreadCount = threadCount;
}

bool IOCPCore::CreateIOCPWorkerthread()
{
	if (m_iocpThreadCount == 0)
	{
		return false;
	}

	m_iocpWorkerThreadRunning = true;

	m_iocpWorkerThreadHandles = new HANDLE[m_iocpThreadCount];

	if (!m_iocpWorkerThreadHandles)
		return false;

	::ZeroMemory(m_iocpWorkerThreadHandles, sizeof(HANDLE) * m_iocpThreadCount);

	for (DWORD i = 0; i < m_iocpThreadCount; i++)
	{
		unsigned int threadId = 0;
		HANDLE hThread = (HANDLE)_beginthreadex(
			nullptr,
			0,
			&IOCPCore::IOCPWorkerThreadProc,
			this,
			0,
			&threadId
		);

		if (!hThread)
		{
			// 스레드 생성 실패
			return false;
		}

		m_iocpWorkerThreadHandles[i] = hThread;
	}

	//Log::log(LogLevel::LOG_INFO, "[%s] createIOCPWorkerthread success\n", __FUNCTION__);

	return true;
}

void IOCPCore::DestroyIOCPWorkerthread()
{
	if (!m_iocpWorkerThreadHandles)
	{
		return;
	}

	m_iocpWorkerThreadRunning = false;

	Logger::Log(LogLevel::LOG_INFO, "[%s] IOCP GQCS 스레드 종료 대기 중...", __FUNCTION__);

	for (DWORD i = 0; i < m_iocpThreadCount; i++)
	{
		if (m_iocpWorkerThreadHandles[i])
		{
			::WaitForSingleObject(m_iocpWorkerThreadHandles[i], INFINITE);
			::CloseHandle(m_iocpWorkerThreadHandles[i]);
			m_iocpWorkerThreadHandles[i] = nullptr;
		}

		//Log::log(LogLevel::LOG_INFO, "[%s] destroyIOCPWorkerthread success\n", __FUNCTION__);
	}

	Logger::Log(LogLevel::LOG_INFO, "[%s] IOCP GQCS 스레드 종료 확인", __FUNCTION__);

	delete[] m_iocpWorkerThreadHandles;
	m_iocpWorkerThreadHandles = nullptr;
}

void IOCPCore::RequestIOCPThreadTerminate()
{
	// IOCP 에 종료 신호 보내기
	// GQCS Worker Thread 를 '우아한 종료' 시키기 위해서라면
	// 클라이언트쪽에서 연결 해제를 요청해야한다.
	// 하지만 그럴 수 없는 상황이라면 서버가 종료 메시지를 Post 해주도록 하자.
	// 스레드를 종료시킨 후에 IOCP Handle 을 Close 해주도록 하자.

	if (!m_iocpWorkerThreadHandles)
	{
		return;
	}

	if (m_iocpHandle == INVALID_HANDLE_VALUE)
	{
		return;
	}

	Logger::Log(LogLevel::LOG_INFO, "[%s] IOCP GQCS 스레드에 종료 코드 전송 중...", __FUNCTION__);

	for (DWORD i = 0; i < m_iocpThreadCount; i++)
	{
		if (m_iocpWorkerThreadHandles[i])
		{
			BOOL bResult = ::PostQueuedCompletionStatus(m_iocpHandle, 0, TERMINATE_CODE, NULL);

			if (!bResult)
			{
				DWORD error = ::GetLastError();

				Logger::Log(LogLevel::LOG_ERROR, "[%s] IOCP GQCS 스레드에 종료 코드 전송 실패! (ERROR CODE : %d)", __FUNCTION__, error);

				__debugbreak();
			}
		}
	}
}

unsigned int __stdcall IOCPCore::IOCPWorkerThreadProc(LPVOID param)
{
	IOCPCore* self = static_cast<IOCPCore*>(param);

	self->IOCPWorkerThreadLoop();

	return 0;
}

void IOCPCore::IOCPWorkerThreadLoop()
{
	const LONG iocpThreadId = ::InterlockedIncrement(&m_threadCounter) - 1;

	BOOL completionStatus = FALSE;
	DWORD bytesTransferred = 0;
	ULONG_PTR completionKey = 0;
	LPOVERLAPPED overlapped = nullptr;

	Logger::Log(LogLevel::LOG_INFO, "[%s][Thread ID : %d] IOCP GQCS 스레드 생성 중...", __FUNCTION__, iocpThreadId);

	while (m_iocpWorkerThreadRunning)
	{
		completionStatus = FALSE;
		bytesTransferred = 0;
		completionKey = 0;
		overlapped = nullptr;

		// GetQueuedCompletionStatusEx 라는 확장형 함수도 있다. 나중에 사용해보자
		completionStatus = ::GetQueuedCompletionStatus(
			m_iocpHandle,
			&bytesTransferred,
			&completionKey,
			&overlapped,
			INFINITE);

		if (completionKey == TERMINATE_CODE)
		{
			// 서버가 GQCS Thread 종료를 요청한 경우
			// Terminate Code 가 수신 되었으므로 스레드를 종료시킨다.

			Logger::Log(LogLevel::LOG_INFO, "[%s][Thread ID : %d] IOCP GQCS Terminate Code 송신", __FUNCTION__, iocpThreadId);

			//Log::log(LogLevel::LOG_INFO, "[%s] [Thread : %d] Terminate Code Enabled. worker closed\n", __FUNCTION__, iocpThreadID);
			m_iocpWorkerThreadRunning = false;
			break;
		}

		if (!m_iocpWorkerThreadRunning)
		{
			// 스레드 종료 시그널이 수신 되었으므로 스레드 종료를 위해 while 탈출한다.

			//Log::log(LogLevel::LOG_INFO, "[%s] [Thread : %d] IOCPWorkerThreadLoop Terminated - ErroCode : %d, %d \n", __FUNCTION__, iocpThreadID, WSAGetLastError(), GetLastError());

			break;
		}

		HandleCompletion(completionKey, overlapped, bytesTransferred, completionStatus);
	}

	Logger::Log(LogLevel::LOG_INFO, "[%s][Thread ID : %d] IOCP GQCS 스레드를 종료 합니다.", __FUNCTION__, iocpThreadId);
}

bool IOCPCore::RegisterSocketToIOCP(ULONG_PTR completionKey, SOCKET socket)
{
	if (::CreateIoCompletionPort(
		(HANDLE)socket,
		GetIOCPHandle(),
		completionKey,
		0) == NULL)
	{
		Logger::Log(LogLevel::LOG_INFO, "[%s][socket : %d] IOCP Register Fail! (ERROR CODE : %d)", __FUNCTION__, (int)socket, ::GetLastError());

		//Log::Error("Failed to associate socket with IOCP");
		return false;
	}

	return true;
}

