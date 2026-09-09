#pragma once

#include "IOCPCore.h"
#include "../Job/JobDefs.h"
#include "../Protocol/SystemPacket.h"
#include "../Buffer/SessionBufferConfig.h"
#include "../Session/ConnectionPolicyConfig.h"

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
#include "../Memory/EngineMemoryPoolFwd.h"
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
	bool StartServer(const char* ipAddress, const uint16_t port, const uint32_t maxConnectionCount,
		const SessionBufferConfig& bufferConfig = SessionBufferPreset::Server(),
		const ConnectionPolicyConfig& policyConfig = ConnectionPolicyConfig());
	void StopServer();

private:
	SOCKET m_serverSocket = INVALID_SOCKET;

	SessionManager* m_sessionManager = nullptr;
	HybridSendPacketPool* m_hybridSendPacketPool = nullptr;
	ReadySessionQueue* m_readySessionQueue = nullptr;
	ReadySessionScheduler* m_readySessionScheduler = nullptr;
	HeartbeatThread* m_heartbeatThread = nullptr;
	PacketHandlerTable* m_packetHandlerTable = nullptr;
	EngineMemoryPool* m_jobMemoryPool = nullptr;
	EngineMemoryPool* m_packetMemoryPool = nullptr;
	EngineMemoryPool* m_generalMemoryPool = nullptr;

	HandlerContext m_handlerContext = {};

	volatile LONG m_serverShutdownRequested = FALSE;

	// 지금 리슨 소켓에 걸려 있는 AcceptEx 개수.
	//
	// 이게 0 이 되면 서버는 살아 있는 채로 새 접속을 하나도 받지 못한다.
	// 예전에는 그 상태와 정상 동작이 로그로 구분되지 않았다.
	volatile LONG m_postedAcceptCount = 0;

	// 유지하려는 개수. 자원 부족 등으로 걸기가 실패해 이보다 줄어들면
	// 주기 점검이 빈 슬롯을 다시 채운다.
	uint32_t m_desiredAcceptCount = 0;

	// 한 번만 크게 울리기 위한 래치. 매 실패마다 같은 경보를 쏟지 않는다.
	volatile LONG m_acceptStarvationReported = FALSE;

	// 접속 수용 정책. StartServer 에서 받아 보관한다.
	ConnectionPolicyConfig m_connectionPolicy;

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
	bool PostAccept(AcceptSession* acceptSession);

	// 걸기가 실패해 비어 버린 슬롯을 다시 채운다.
	//
	// 주기 점검 스레드에서만 호출한다. accept 슬롯의 상태는 원자적이지
	// 않으므로, 여러 스레드가 같은 빈 슬롯을 동시에 집으면 같은
	// OVERLAPPED 로 AcceptEx 를 두 번 걸어 완료 통지가 짝을 잃는다.
	// 슬롯 자신의 완료를 처리하는 워커가 그 슬롯을 다시 거는 경로와는
	// 겹치지 않는다 — 그 워커는 실패한 뒤에야 슬롯을 비운 채로 떠난다.
	void RefillAcceptSlots();

	// 걸려 있는 AcceptEx 가 0 이면 크게 울린다. 걸기를 시도한 직후에 부른다.
	void ReportAcceptStarvationIfNeeded();
	bool SendSystemAuthResponse(ClientSession* session, SYSTEM_AUTH_RESULT authResult);
	// 시스템 패킷 처리 결과.
	//
	// 예전에는 bool 이라 "정상적인 거절" 과 "처리 실패" 가 구분되지 않았다.
	// 그래서 거절 응답을 보낸 세션이 true 로 돌아가 살아남았고, 만석으로
	// 거절한 접속이 하트비트 타임아웃까지 슬롯을 붙들었다.
	enum class SystemPacketResult
	{
		Ok,        // 처리 완료. 세션 유지
		Rejected,  // 정상적인 거절. 응답을 보냈으니 세션을 정리한다 (오류 아님)
		Failed,    // 처리 실패. 세션을 정리한다 (오류)
	};

	SystemPacketResult HandleSystemPacket(ClientSession* session, uint16_t packetId, const char* packetData, uint32_t packetSize);

protected:
	ReadySessionQueue* GetReadySessionQueue() const;
	PacketHandlerTable* GetPacketHandlerTable() const;

public:
	// 지금 걸려 있는 AcceptEx 개수와 유지 목표. 운영 지표라 서비스가 읽는다.
	// 이 값이 목표보다 오래 낮게 머물면 자원 부족을 의심해야 하고,
	// 0 이면 새 접속을 전혀 받지 못하는 상태다.
	uint32_t GetPostedAcceptCount() const;
	uint32_t GetDesiredAcceptCount() const;

protected:
	EngineMemoryPool* GetJobMemoryPool() const;
	EngineMemoryPool* GetPacketMemoryPool() const;
	EngineMemoryPool* GetGeneralMemoryPool() const;
	const HandlerContext& GetHandlerContext() const;
	virtual void* GetServiceContext();

protected:
	virtual void OnClientConnect(ISession* session) = 0;
	virtual void OnClientDisconnect(ISession* session) = 0;
	virtual void OnReceive(ISession* session, uint16_t packetId, const char* packetData, uint32_t packetSize) = 0;
	virtual void OnSend(ISession* session, uint32_t bytesTransferred) = 0;
};





