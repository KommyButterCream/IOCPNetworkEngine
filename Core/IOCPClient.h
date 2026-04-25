#pragma once

#include "IOCPCore.h"
#include "../Session/ISessionEvent.h"
#include "../Job/JobDefs.h"

#include <stdint.h>


enum class IO_OPERATION;

struct OverlappedEx;

class ISession;
class ClientSession;
class HybridSendPacketPool;
class SlabMemoryPool;
class ClientSessionScheduler;
class PacketHandlerTable;
class SessionContext;

#ifdef BUILD_IOCP_ENGINE_DLL
#define IOCP_ENGINE_API __declspec(dllexport)
#else
#define IOCP_ENGINE_API __declspec(dllimport)
#endif

#define INET_ADDRSTRLEN  22

class IOCP_ENGINE_API IOCPClient : public IOCPCore, public ISessionEvent
{
public:
	IOCPClient();
	virtual ~IOCPClient();

public:
	bool StartClient(const char* serverIp, const uint16_t port);
	void StopClient();

private:
	// Destory Flag
	LONG m_destroyFlag = 0;

	SOCKET m_clientSocket = INVALID_SOCKET;

	ClientSession* m_session = nullptr;

	HybridSendPacketPool* m_hybridSendPacketPool = nullptr;
	SlabMemoryPool* m_jobMemoryPool = nullptr;
	SlabMemoryPool* m_packetMemoryPool = nullptr;
	SlabMemoryPool* m_generalMemoryPool = nullptr;
	ClientSessionScheduler* m_clientSessionScheduler = nullptr;
	PacketHandlerTable* m_packetHandlerTable = nullptr;
	HandlerContext m_handlerContext = {};

	// IP Address & Port
	char m_serverIPAddress[INET_ADDRSTRLEN] = { 0, };
	uint16_t m_serverPort = 0;

private:
	// for ConnectEx
	using LPFN_CONNECTEX = BOOL(__stdcall*)(
		_In_ SOCKET s,
		_In_reads_bytes_(namelen) const struct sockaddr FAR* name,
		_In_ int namelen,
		_In_reads_bytes_opt_(dwSendDataLength) PVOID lpSendBuffer,
		_In_ DWORD dwSendDataLength,
		_Out_ LPDWORD lpdwBytesSent,
		_Inout_ LPOVERLAPPED lpOverlapped
		);

	// ConnectEx 함수 포인터
	LPFN_CONNECTEX m_connectEx = nullptr;

private:
	// GQCS 에 통지 받은 IO 처리
	void HandleCompletion(ULONG_PTR completionKey, LPOVERLAPPED overlapped, DWORD bytesTransferred, BOOL completionStatus) override;
	void HandleSocketError(OverlappedEx* overlappedEx, ISession* session, int errorCode, IO_OPERATION ioOperation) override;

	void HandleConnect(uint32_t sessionId, DWORD bytesTransferred);
	void HandleConnectCancelled(OverlappedEx* overlappedEx, ISession* session);

	void HandleRecv(OverlappedEx* overlappedEx, ISession* session, DWORD bytesTransferred);
	void HandleRecvCancelled(OverlappedEx* overlappedEx, ISession* session);

	void HandleSend(OverlappedEx* overlappedEx, ISession* session, DWORD bytesTransferred);
	void HandleSendCancelled(OverlappedEx* overlappedEx, ISession* session);

	void HandleSessionDisconnected(OverlappedEx* overlappedEx, ISession* session, DWORD bytesTransferred);

private:
	bool CreateConnectSocket();
	void DestroyConnectSocket();

	bool BindClientSocket(SOCKET clientSocket);
	bool ListenServerSocket(SOCKET clientSocket);

	bool InitializeGUIDConnectEx(SOCKET clientSocket);
	void FinalizeGUIDConnectEx();

	bool PrepareConnect();
	bool PostConnect(ISession* session);
	bool SendSystemAuthRequest(ClientSession* session);
	bool HandleSystemPacket(ClientSession* session, uint16_t packetId, const char* packetData, uint32_t packetSize);

	void OnDisconnectRequest(ISession* session) override;

public:
	PacketHandlerTable* GetPacketHandlerTable() const;
	SlabMemoryPool* GetJobMemoryPool() const;
	SlabMemoryPool* GetPacketMemoryPool() const;
	SlabMemoryPool* GetGeneralMemoryPool() const;
	const HandlerContext& GetHandlerContext() const;
	ClientSession* GetClientSession() const;


protected:
	virtual void OnClientConnect(ISession* session) {};
	virtual void OnClientDisconnect(ISession* session) {};
	virtual void OnReceive(ISession* session, uint16_t packetId, const char* packetData, uint32_t packetSize) {};
	virtual void OnSend(ISession* session, uint32_t bytesTransferred) {};
};





