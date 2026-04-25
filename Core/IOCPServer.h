#pragma once

#include "IOCPCore.h"
#include "../Job/JobDefs.h"
#include "../Protocol/SystemPacket.h"

#include <stdint.h>

enum class IO_OPERATION;

class ISession;
class ClientSession;
class AcceptSession;
class PacketHandlerTable;
class HybridSendPacketPool;
class SessionManager;
class ReadySessionQueue;
class ReadySessionScheduler;
class SlabMemoryPool;
class HeartbeatThread;

struct OverlappedEx;

#ifdef BUILD_IOCP_ENGINE_DLL
#define IOCP_ENGINE_API __declspec(dllexport)
#else
#define IOCP_ENGINE_API __declspec(dllimport)
#endif

class IOCP_ENGINE_API IOCPServer : public IOCPCore
{
public:
	IOCPServer();
	virtual ~IOCPServer();

public:
	bool StartServer(const char* ipAddress, const uint16_t port, const uint32_t maxConnectionCount);
	void StopServer();

private:
	SOCKET m_serverSocket = INVALID_SOCKET;

	SessionManager* m_sessionManager = nullptr;
	HybridSendPacketPool* m_hybridSendPacketPool = nullptr;
	ReadySessionQueue* m_readySessionQueue = nullptr;
	ReadySessionScheduler* m_readySessionScheduler = nullptr;
	HeartbeatThread* m_heartbeatThread = nullptr;
	PacketHandlerTable* m_packetHandlerTable = nullptr;
	SlabMemoryPool* m_jobMemoryPool = nullptr;
	SlabMemoryPool* m_packetMemoryPool = nullptr;
	SlabMemoryPool* m_generalMemoryPool = nullptr;

	HandlerContext m_handlerContext = {};

	volatile LONG m_serverShutdownRequested = FALSE;
private:
	// for AcceptEX
	using LPFN_ACCEPTEX = BOOL(__stdcall*)(
		_In_ SOCKET sListenSocket,
		_In_ SOCKET sAcceptSocket,
		_Out_writes_bytes_(dwReceiveDataLength + dwLocalAddressLength + dwRemoteAddressLength) PVOID lpOutputBuffer,
		_In_ DWORD dwReceiveDataLength,
		_In_ DWORD dwLocalAddressLength,
		_In_ DWORD dwRemoteAddressLength,
		_Out_ LPDWORD lpdwBytesReceived,
		_Inout_ LPOVERLAPPED lpOverlapped
		);

	// for GetAcceptEXSockAddrs
	using LPFN_GETACCEPTEXSOCKADDRS = VOID(__stdcall*)(
		_In_reads_bytes_(dwReceiveDataLength + dwLocalAddressLength + dwRemoteAddressLength) PVOID lpOutputBuffer,
		_In_ DWORD dwReceiveDataLength,
		_In_ DWORD dwLocalAddressLength,
		_In_ DWORD dwRemoteAddressLength,
		_Outptr_result_bytebuffer_(*LocalSockaddrLength) struct sockaddr** LocalSockaddr,
		_Out_ LPINT LocalSockaddrLength,
		_Outptr_result_bytebuffer_(*RemoteSockaddrLength) struct sockaddr** RemoteSockaddr,
		_Out_ LPINT RemoteSockaddrLength
		);


	// AcceptEx 함수 포인터
	LPFN_ACCEPTEX m_acceptEx = nullptr;

	// GetAcceptEXSockAddrs 함수 포인터
	LPFN_GETACCEPTEXSOCKADDRS m_getAcceptExSockAddrs = nullptr;

private:
	// GQCS 에 통지 받은 IO 처리
	void HandleCompletion(ULONG_PTR completionKey, LPOVERLAPPED overlapped, DWORD bytesTransferred, BOOL completionStatus) override;
	void HandleSocketError(OverlappedEx* overlappedEx, ISession* session, int errorCode, IO_OPERATION ioOperation) override;

	void HandleAccept(uint32_t sessionId, DWORD bytesTransferred);
	void HandleAcceptIOCancelled(uint32_t sessionId);
	void HandleRecv(OverlappedEx* overlappedEx, ISession* session, DWORD bytesTransferred);
	void HandleRecvCancelled(OverlappedEx* overlappedEx, ISession* session);
	void HandleSend(OverlappedEx* overlappedEx, ISession* session, DWORD bytesTransferred);
	void HandleSendCancelled(OverlappedEx* overlappedEx, ISession* session);
	void HandleSessionDisconnected(OverlappedEx* overlappedEx, ISession* session, DWORD bytesTransferred);

private:
	bool CreateListenSocket(const char* ipAddress, uint16_t port);
	void DestroyListenSocket();

	bool BindServerSocket(SOCKET serverSocket, const char* ipAddress, uint16_t port);
	bool ListenServerSocket(SOCKET serverSocket);

	bool InitializeGUIDAcceptEx(SOCKET serverSocket);
	void FinalizeGUIDAcceptEx();
	bool CancelAllAcceptIO();
	bool PrepareAccept();
	bool PrepareAccept(uint32_t sessionId);
	bool PostAccept(ISession* session);
	bool SendSystemAuthResponse(ClientSession* session, SYSTEM_AUTH_RESULT authResult);
	bool HandleSystemPacket(ClientSession* session, uint16_t packetId, const char* packetData, uint32_t packetSize);

protected:
	ReadySessionQueue* GetReadySessionQueue() const;
	PacketHandlerTable* GetPacketHandlerTable() const;
	SlabMemoryPool* GetJobMemoryPool() const;
	SlabMemoryPool* GetPacketMemoryPool() const;
	SlabMemoryPool* GetGeneralMemoryPool() const;
	const HandlerContext& GetHandlerContext() const;

protected:
	virtual void OnClientConnect(ISession* session) = 0;
	virtual void OnClientDisconnect(ISession* session) = 0;
	virtual void OnReceive(ISession* session, uint16_t packetId, const char* packetData, uint32_t packetSize) = 0;
	virtual void OnSend(ISession* session, uint32_t bytesTransferred) = 0;
};





