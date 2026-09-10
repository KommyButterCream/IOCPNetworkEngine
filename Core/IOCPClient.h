#pragma once

#include "IOCPCore.h"
#include "../Session/ISessionEvent.h"
#include "../Job/JobDefs.h"
#include "../Buffer/SessionBufferConfig.h"

#include <stdint.h>


enum class IO_OPERATION;

struct OverlappedEx;

class ISession;
class ClientSession;
class HybridSendPacketPool;
#include "../Memory/EngineMemoryPoolFwd.h"
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
	bool StartClient(const char* serverIp, const uint16_t port,
		const SessionBufferConfig& bufferConfig = SessionBufferPreset::Client());
	void StopClient();

private:
	// Destory Flag
	LONG m_destroyFlag = 0;

	SOCKET m_clientSocket = INVALID_SOCKET;

	ClientSession* m_session = nullptr;

	HybridSendPacketPool* m_hybridSendPacketPool = nullptr;
	EngineMemoryPool* m_jobMemoryPool = nullptr;
	EngineMemoryPool* m_packetMemoryPool = nullptr;
	EngineMemoryPool* m_generalMemoryPool = nullptr;
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
	// 아래 핸들러들의 세션 인자는 전부 ClientSession 이다.
	// 이 클라이언트가 가진 세션은 m_session 하나뿐이다.
	void HandleSocketError(OverlappedEx* overlappedEx, ClientSession* session, int errorCode, IO_OPERATION ioOperation);

	void HandleConnect(uint32_t sessionId, DWORD bytesTransferred);
	void HandleConnectCancelled(OverlappedEx* overlappedEx, ClientSession* session);

	void HandleRecv(OverlappedEx* overlappedEx, ClientSession* session, DWORD bytesTransferred);
	void HandleRecvCancelled(OverlappedEx* overlappedEx, ClientSession* session);

	void HandleSend(OverlappedEx* overlappedEx, ClientSession* session, DWORD bytesTransferred);
	void HandleSendCancelled(OverlappedEx* overlappedEx, ClientSession* session);

	void HandleSessionDisconnected(OverlappedEx* overlappedEx, ClientSession* session, DWORD bytesTransferred);

private:
	bool CreateConnectSocket();
	void DestroyConnectSocket();

	bool BindClientSocket(SOCKET clientSocket);
	bool ListenServerSocket(SOCKET clientSocket);

	bool InitializeGUIDConnectEx(SOCKET clientSocket);
	void FinalizeGUIDConnectEx();

	bool PrepareConnect();
	bool PostConnect(ClientSession* clientSession);
	bool SendSystemAuthRequest(ClientSession* session);
	bool HandleSystemPacket(ClientSession* session, uint16_t packetId, const char* packetData, uint32_t packetSize);

	void OnDisconnectRequest(ISession* session) override;

	// ISessionEvent 의 시그니처가 ISession* 로 고정돼 있어 기반 타입이
	// 들어오는 유일한 지점이다. 그 포인터가 정말 이 클라이언트의 세션인지
	// 확인해서 돌려준다. 아니면 nullptr 과 함께 위반을 남긴다.
	ClientSession* ResolveOwnSession(ISession* session, const char* calledFrom);

public:
	PacketHandlerTable* GetPacketHandlerTable() const;
	EngineMemoryPool* GetJobMemoryPool() const;
	EngineMemoryPool* GetPacketMemoryPool() const;
	EngineMemoryPool* GetGeneralMemoryPool() const;
	const HandlerContext& GetHandlerContext() const;
	ClientSession* GetClientSession() const;
	virtual void* GetServiceContext();


protected:
	virtual void OnClientConnect(ISession* session) {};
	virtual void OnSessionEstablished(ISession* session) {};
	virtual void OnClientDisconnect(ISession* session) {};
	virtual void OnReceive(ISession* session, uint16_t packetId, const char* packetData, uint32_t packetSize) {};
	virtual void OnSend(ISession* session, uint32_t bytesTransferred) {};
};





