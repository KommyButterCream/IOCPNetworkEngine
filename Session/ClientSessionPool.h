#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>
#include <stdint.h>
#include "ISessionEvent.h"

#include "../Buffer/SessionBufferConfig.h"

struct SessionNode;

class ISession;
class ClientSession;
class HybridSendPacketPool;
#include "../Memory/EngineMemoryPoolFwd.h"

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

	// 생성자는 실패를 반환할 수 없다. 세션 하나라도 준비에 실패하면 여기가
	// false 로 남고, SessionManager 가 그걸 보고 초기화를 실패시킨다.
	// 이게 없으면 세션을 하나도 못 잡는 서버가 정상 기동한 것처럼 보인다.
	bool m_ready = false;

public:
	explicit ClientSessionPool(uint32_t capacity, HybridSendPacketPool* hybridSendPacketPool, EngineMemoryPool* jobMemoryPool, EngineMemoryPool* packetMemoryPool, EngineMemoryPool* generalMemoryPool, const SessionBufferConfig& bufferConfig);
	~ClientSessionPool();

	bool IsReady() const { return m_ready; }

	// 세션 획득
	ISession* Acquire();

	// 세션 반환
	void Release(ISession* session);

	// 세션 수량 반환
	uint32_t GetSessionCount() const;

	// 세션이 모두 Connected 되어서 사용 중인지 bool 반환
	bool IsSessionFull() const;

	// 지금 임대되어 있는 세션 수.
	// 노드 상태를 훑어서 센다. 별도 카운터를 두면 예외 경로에서 어긋날 수
	// 있고, 이 함수는 접속/인증 때만 불리므로 O(capacity) 로 충분하다.
	uint32_t GetInUseCount() const;

	// 같은 원격 주소에서 온 접속 수. 접속 폭주 방어용.
	uint32_t CountSessionsFromAddress(const char* ipAddress) const;

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
