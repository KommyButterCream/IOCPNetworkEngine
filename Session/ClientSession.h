#pragma once

#include <stdint.h>


#include "ISession.h"
#include "BaseSession.h"
#include "SessionContext.h"

#include "../Network/OverlappedEx.h"
#include "../Protocol/PacketID.h"

struct Job;
struct SendPacketBuffer;

class SessionJobQueue;
class RecvPacketBuffer;
class SendPacketQueue;
class SendPacketPool;
class HybridSendPacketPool;
class SlabMemoryPool;
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
	SlabMemoryPool* m_jobMemoryPool = nullptr;
	SlabMemoryPool* m_packetMemoryPool = nullptr;
	SlabMemoryPool* m_generalMemoryPool = nullptr;

	// Client Role
	OverlappedEx m_connectOverlapped{};

	// Client / Server Role
	OverlappedEx m_recvOverlapped{};
	RecvPacketBuffer* m_recvPacketBuffer = nullptr;
	ULONGLONG m_lastRecvBufferFullTime = 0;

	OverlappedEx m_sendOverlapped{};
	SendPacketQueue* m_sendPacketQueue = nullptr;
	SendPacketPool* m_sendPacketPool = nullptr;
	SendPacketBuffer* m_currentSendPacket = nullptr;
	Job* m_currentJob = nullptr;
	uint32_t m_sendOffset = 0;
	SessionJobQueue* m_jobQueue = nullptr;

	volatile LONG m_sending = 0; // sending flag: 0 = not sending, 1 = sending (Interlocked)
	volatile LONG m_processing = 0; // processing flag: 0 = idle, 1 = being processed by a worker

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
	bool InitializeMemoryPool(HybridSendPacketPool* hybridSendPacketPool, SlabMemoryPool* jobMemoryPool, SlabMemoryPool* packetMemoryPool, SlabMemoryPool* generalMemoryPool);

	bool IsReady() const;
	bool IsConnected() const;
	bool IsEstablished() const;
	bool IsTransportConnected() const;

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

	void HandleSocketError(int errorCode, IO_OPERATION ioOperation);

	void UpdateProcessingFlag(LONG value);
	bool IsProcessingReady();

	void SetSessionContext(SessionContext* sessionContext);
	SessionContext* GetSessionContext() const;

	void SetCurrentJob(Job* job);
	void ClearCurrentJobData();

	void SetRemoteAddress(const char* ipAddress, uint16_t port);
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

