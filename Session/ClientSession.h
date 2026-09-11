#pragma once

#include <stdint.h>


#include "ISession.h"
#include "BaseSession.h"
#include "SessionContext.h"

#include "../Network/OverlappedEx.h"
#include "../Protocol/PacketID.h"
#include "../Buffer/SendPacketEntry.h"
#include "../Buffer/SessionBufferConfig.h"

struct Job;

class SessionJobQueue;
class RecvPacketBuffer;
class SendPacketQueue;
#include "../Memory/EngineMemoryPoolFwd.h"
class ISessionEvent;

#ifdef BUILD_IOCP_ENGINE_DLL
#define IOCP_ENGINE_API __declspec(dllexport)
#else
#define IOCP_ENGINE_API __declspec(dllimport)
#endif

#define INET_ADDRSTRLEN  22

class IOCP_ENGINE_API ClientSession final : public BaseSession
{
public:
	ClientSession();
	~ClientSession() override;

	// ClientSession 전용 멤버 변수
private:
	EngineMemoryPool* m_jobMemoryPool = nullptr;
	EngineMemoryPool* m_packetMemoryPool = nullptr;
	EngineMemoryPool* m_generalMemoryPool = nullptr;

	// Client Role
	OverlappedEx m_connectOverlapped{};

	// Client / Server Role
	OverlappedEx m_recvOverlapped{};
	RecvPacketBuffer* m_recvPacketBuffer = nullptr;
	ULONGLONG m_lastRecvBufferFullTime = 0;

	// 이 세션의 버퍼 크기 정책. InitializeMemoryPool 에서 받아 보관한다.
	// 송신 상한 검사가 여기를 본다.
	SessionBufferConfig m_bufferConfig;

	OverlappedEx m_sendOverlapped{};
	SendPacketQueue* m_sendPacketQueue = nullptr;

	// 큐에서 꺼내 전송 중인 엔트리. 큐 밖에 있는 유일한 엔트리이고,
	// 다 쓰면 m_sendPacketQueue->ReleaseEntry 로 되돌린다.
	//
	// 예전에는 전용 서술자 풀 포인터(m_sendPacketPool)를 따로 들고 있었다.
	// 패킷 반납과 서술자 반납을 세션이 각각 불러야 했기 때문이다. 반납을
	// 큐 한 곳으로 모으면서 그 멤버가 필요 없어졌다.
	SendPacketEntry* m_currentSendPacket = nullptr;
	Job* m_currentJob = nullptr;
	uint32_t m_sendOffset = 0;
	SessionJobQueue* m_jobQueue = nullptr;

	volatile LONG m_sending = 0; // sending flag: 0 = not sending, 1 = sending (Interlocked)
	volatile LONG m_processing = 0; // processing flag: 0 = idle, 1 = being processed by a worker

	// 서비스에 접속을 알렸는가. 종료 통지의 짝을 맞추는 래치다.
	// (MarkServiceConnectNotified / ConsumeServiceConnectNotified 주석 참고)
	volatile LONG m_serviceConnectNotified = 0;

	ISessionEvent* m_eventHandler = nullptr;

	// 서비스 로직별 세션 컨텍스트
	SessionContext* m_sessionContext = nullptr;

	// IP Address & Port
	char m_clientIPAddress[INET_ADDRSTRLEN] = { 0, };
	uint16_t m_clientPort = 0;
	volatile LONGLONG m_lastRecvTick = 0;
	volatile LONGLONG m_lastHeartbeatTick = 0;


	// Override
public:
	// --- 기본 동작 ---
	bool Initialize(SESSION_ROLE sessionType, uint32_t sessionId) override;
	void ResetSession() override;
	void Finalize() override;

	bool OnAccept() override;
	bool OnConnect() override;
	bool OnDisconnect() override;

	// ClientSession 전용 메서드
public:
	bool InitializeMemoryPool(EngineMemoryPool* sendQueueMemoryPool, EngineMemoryPool* jobMemoryPool, EngineMemoryPool* packetMemoryPool, EngineMemoryPool* generalMemoryPool, const SessionBufferConfig& bufferConfig);

	bool IsReady() const;
	// 두 단계다. IsTransportConnected 는 소켓이 붙었는가,
	// IsEstablished 는 인증까지 끝났는가. (설명은 .cpp 주석)
	bool IsTransportConnected() const;
	bool IsEstablished() const;

	// --- 서비스 접속/종료 통지의 짝 맞추기 ---
	//
	// OnClientConnect 를 받은 세션만 OnClientDisconnect 를 받아야 하고,
	// 정확히 한 번만 받아야 한다. 그 조건을 세션에 래치로 들고 있는다.
	//
	// 상태(ServerSessionState 등)로 대신할 수 없다. OnConnect 가 성공해
	// CONNECTED 로 가더라도 그 직후 통지 호출까지 갔는지는 상태에 남지
	// 않는다. 접속 시퀀스가 중간에 실패하면 통지 없이 끝난다.
	void MarkServiceConnectNotified();

	// 종료를 알려야 하는가. true 는 한 스레드에게만 돌아간다.
	//
	// 반납 경로가 여럿이고(소켓 오류 / 좀비 정리 / 서버 종료) 서로 겹칠
	// 수 있으므로, "알릴 차례인가" 와 "표시 지우기" 가 한 번에 일어나야
	// 두 번 알리지 않는다.
	bool ConsumeServiceConnectNotified();

	OverlappedEx& GetConnectOverlapped();

	const RecvPacketBuffer& GetReceiveBuffer() const;
	RecvPacketBuffer& GetReceiveBuffer();

	const SendPacketQueue* GetSendPacketQueue() const;
	SendPacketQueue* GetSendPacketQueue();

	SessionJobQueue& GetJobQueue() const;

	bool PostReceive();
	bool TrySendNext();
	bool PostCurrentSend();
	bool OnSendCompleted(const DWORD bytesTransferred);

	bool EnqueueJob(Job* job, bool& wasEmpty);
	bool SubmitJob(Job* job);
	bool EnqueueSendPacket(void** packetData, uint32_t packetSize);
	bool EnqueueSharedSendPacket(const void* packetData, uint32_t packetSize, SendPacketReleaseFunc releaseFunc, void* releaseContext);

	void HandleSocketError(int errorCode, IO_OPERATION ioOperation);

	void UpdateProcessingFlag(LONG value);
	bool IsProcessingReady();

	void SetSessionContext(SessionContext* sessionContext);
	SessionContext* GetSessionContext() const;

	void SetCurrentJob(Job* job);
	void ClearCurrentJobData();

	void SetRemoteAddress(const char* ipAddress, uint16_t port);

	// 접속 폭주 방어에서 같은 주소의 접속 수를 셀 때 쓴다.
	// 세션이 비어 있으면 빈 문자열이다.
	const char* GetClientIPAddress() const { return m_clientIPAddress; }
	void ClearRemoteAddress();
	void UpdateLastRecvTick();
	void UpdateLastHeartbeatTick();
	uint64_t GetLastRecvTick() const;
	uint64_t GetLastHeartbeatTick() const;
	uint64_t GetLastActiveTick() const;
	bool IsHeartbeatTimedOut(uint64_t nowTick, uint64_t timeout_ms) const;
	void MarkHeartbeatTimeout();
	bool SendSystemHeartbeatRequest();
	bool SendSystemHeartbeatResponse(uint64_t requestTick);

private:
	bool CanSendPacket(PACKET_ID_TYPE packetId) const;

	// InitializeMemoryPool 이 잡은 자원만 되돌린다.
	// 그쪽의 실패 정리와 Finalize 가 함께 쓴다. 부분 생성 상태에서도 안전하다.
	void ReleaseMemoryResources();

public:
	// Callback Func
	void SetEventHandler(ISessionEvent* handler);
	void NotifyDisconnect();


protected:
	virtual void OnClientConnect(ISession* session) {};
	virtual void OnClientDisconnect(ISession* session) {};
	virtual void OnReceive(ISession* session, uint16_t packetId, const char* packetData, uint32_t packetSize) {};
	virtual void OnSend(ISession* session, uint32_t bytesTransferred) {};
};

