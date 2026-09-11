#pragma once

#include "IOCPCore.h"
#include "../Session/ISessionEvent.h"
#include "../Job/JobDefs.h"
#include "../Buffer/SessionBufferConfig.h"

#include <stdint.h>


enum class IO_OPERATION;

struct OverlappedEx;

class ISession;
class BaseSession;
class ClientSession;
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

// ISessionEvent 를 직접 상속하는 이유. (서버는 그렇지 않다)
//
// 세션의 종료 요청(OnDisconnectRequest)을 받는 주체가 서버와 클라이언트에서
// 다르다. 서버는 그 신호로 세션을 풀에 반납해야 하므로 풀을 가진
// SessionManager 가 받는다. 클라이언트는 세션이 m_session 하나뿐이고 반납할
// 풀이 없으니 자기가 직접 받는다.
//
// 그래서 IOCPServer 는 IOCPCore 만 상속하고, 이쪽만 다중 상속이다. 같은
// 이벤트가 두 가지로 배선된 것처럼 보이지만 받아서 할 일이 다르다.
// (어댑터 멤버를 끼워 단일 상속으로 맞출 수도 있으나, 클래스와 간접 호출을
//  하나씩 늘려 가독성 흠 하나를 바꾸는 거래라 하지 않는다)
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

	// 종료 절차를 한 스레드만 수행하게 하는 게이트.
	//
	// OnDisconnectRequest 는 여러 경로에서 동시에 들어온다.
	//   - 앱 스레드의 StopClient()
	//   - ConnectEx 취소 완료 처리(HandleConnectCancelled)
	//   - recv/send 소켓 오류 -> ClientSession::NotifyDisconnect
	//
	// 가드가 없으면 들어온 스레드마다 종료 절차를 중복 수행한다. 소켓을
	// 두 번 닫고 OnDisconnect 를 두 번 부른다.
	// (서버 쪽은 ClientSessionPool 의 poolState CAS 가 같은 역할을 한다)
	//
	// 예전에는 그 중복이 각자 10초짜리 WaitForIOCancelComplete 였다.
	// 이제 그 대기 자체가 없어졌지만(지연 반납) 중복 수행은 여전히
	// 막아야 하므로 게이트는 남는다.
	volatile LONG m_disconnecting = 0;

	SOCKET m_clientSocket = INVALID_SOCKET;

	ClientSession* m_session = nullptr;

	EngineMemoryPool* m_jobMemoryPool = nullptr;
	EngineMemoryPool* m_packetMemoryPool = nullptr;
	EngineMemoryPool* m_generalMemoryPool = nullptr;

	// 송신 큐 엔트리 전용 풀. 분리 이유는 IOCPServer 의 같은 멤버 주석 참고.
	EngineMemoryPool* m_sendQueueMemoryPool = nullptr;
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

	// 접속 완료 후의 소켓 옵션 / OnConnect / 인증 요청까지.
	// 실패를 반환값 하나로 모으려고 떼어냈다. 이유는 서버 쪽
	// RunAcceptedConnectSequence 와 같다.
	bool RunClientConnectSequence();

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

	// 종료 절차의 뒷부분. 소켓을 닫고 세션 상태를 정리한다.
	// OnDisconnectRequest 가 그 자리에서 부르거나(남은 I/O 가 없을 때),
	// 마지막 DecrementIO 가 부른다.
	void CompleteDisconnect(ClientSession* clientSession);

	// BaseSession 이 마지막 DecrementIO 에서 부르는 진입점.
	static void OnReleaseReady(void* context, BaseSession* session);

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

	// 이 클라이언트 세션이 들고 있는 미완료 I/O 수.
	// 조용한 상태에서 0 으로 돌아오지 않으면 Increment 와 Decrement 의
	// 짝이 맞지 않는다는 신호다.
	uint32_t GetOutstandingIOCount() const;
	virtual void* GetServiceContext();


protected:
	// 받은 패킷을 세션의 Job 큐에 올린다. OnReceive 에서 부르는 것이 정상
	// 사용이다. 계약은 IOCPServer::SubmitPacketJob 과 같다 —
	// packetData 의 소유권은 성공/실패와 무관하게 이 함수가 가져간다.
	//
	// 클라이언트는 세션이 하나이고 소비자도 하나(ClientSessionScheduler)라
	// ReadySessionQueue 도 처리 중 플래그도 쓰지 않는다. 큐에 넣는 것으로
	// 끝이고, 잠들어 있는 소비자를 깨우는 일은 EnqueueJob 이 한다.
	bool SubmitPacketJob(ISession* session, uint16_t packetId, const char* packetData, uint32_t packetSize);

	virtual void OnClientConnect(ISession* session) {};
	virtual void OnSessionEstablished(ISession* session) {};
	virtual void OnClientDisconnect(ISession* session) {};
	virtual void OnReceive(ISession* session, uint16_t packetId, const char* packetData, uint32_t packetSize) {};
	virtual void OnSend(ISession* session, uint32_t bytesTransferred) {};
};





