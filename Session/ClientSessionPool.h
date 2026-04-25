#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>
#include <stdint.h>
#include "ISessionEvent.h"

struct SessionNode;

class ISession;
class ClientSession;
class HybridSendPacketPool;
class SlabMemoryPool;

class ClientSessionPool
{
private:
	ClientSession* m_sessions = nullptr;		// 주소가 연속된 Session 객체 배열 생성
	SessionNode* m_nodes = nullptr;			// 연속 배열
	SessionNode* m_freeList = nullptr;		// 프리 리스트 헤드
	uint32_t m_capacity = 0;
	mutable SRWLOCK m_lock = SRWLOCK_INIT;
	ISessionEvent* m_eventHandler = nullptr;
	CloseSocketFunc m_closeSocketFunc = nullptr;

public:
	explicit ClientSessionPool(uint32_t capacity, HybridSendPacketPool* hybridSendPacketPool, SlabMemoryPool* jobMemoryPool, SlabMemoryPool* packetMemoryPool, SlabMemoryPool* generalMemoryPool);
	~ClientSessionPool();

	// 세션 획득
	ISession* Acquire();

	// 세션 반환
	void Release(ISession* session);

	// 세션 수량 반환
	uint32_t GetSessionCount() const;

	// 세션이 모두 Connected 되어서 사용 중인지 bool 반환
	bool IsSessionFull() const;

	// 세션 아이디 기반 세션 객체 반환
	ISession* GetSession(const uint32_t sessionId);

	// 세션 연결 해제
	void RequestAllRecvSendIOCancel();
	bool WaitForAllRecvSendIOCancelComplete(const uint32_t timeout_ms);
	void DisconnectAllSessions();
	uint32_t SendHeartbeatRequests();
	uint32_t DisconnectZombieSessions(uint64_t nowTick, uint64_t heartbeatTimeout_ms);

	// Session Close 함수 포인터 설정
	// closesocket 을 하나의 함수에서만 수행되도록 강제!
	void SetSocketCloseFunc(CloseSocketFunc closeSocketFunc);

	void SetEventHandler(ISessionEvent* handler);
};
