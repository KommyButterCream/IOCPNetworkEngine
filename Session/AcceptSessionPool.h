#pragma once

#include <stdint.h>
#include "ISessionEvent.h"

class AcceptSession;

class AcceptSessionPool
{
private:
	AcceptSession* m_sessions = nullptr;		 // 주소가 연속된 Session 객체 배열 생성
	uint32_t m_capacity = 0;
	CloseSocketFunc m_closeSocketFunc = nullptr;

public:
	explicit AcceptSessionPool(uint32_t capacity);
	~AcceptSessionPool();

	// 세션 아이디로 풀의 세션 직접 접근
	AcceptSession* GetSession(const uint32_t sessionId);

	// 세션 수량 반환
	uint32_t GetSessionCount() const;

	// 세션 연결 해제
	void RequestAllAcceptIOCancel();
	bool WaitForAllAcceptIOCancelComplete(const uint32_t timeout_ms);

	// Session Close 함수 포인터 설정
	// closesocket 을 하나의 함수에서만 수행되도록 강제!
	void SetSocketCloseFunc(CloseSocketFunc closeSocketFunc);
};
