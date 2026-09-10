#pragma once

#include <stdint.h>

#include "BaseSession.h"
#include "../Network/OverlappedEx.h"

class AcceptSession final : public BaseSession
{
public:
	AcceptSession();
	~AcceptSession() override;

private:
	OverlappedEx m_acceptOverlapped = {};
	char m_acceptBuffer[sizeof(SOCKADDR_IN) + 16 + sizeof(SOCKADDR_IN) + 16] = {};

	// 이 슬롯을 지금 누가 쓰고 있는가. AcceptEx 를 걸 권리다.
	//
	// AcceptEx 를 다시 거는 주체가 둘이다 — accept 완료를 처리하는 IOCP
	// 워커와, 비어 버린 슬롯을 채우는 주기 점검(RefillAcceptSlots).
	// 예전에는 둘 다 GetAcceptSessionState() == ACCEPT_READY 만 보고
	// 판단했는데, 그 상태는 평범한 멤버라 원자적이지 않다.
	//
	// HandleAccept 의 거절 경로는 ResetSession(상태가 ACCEPT_READY 가 된다)
	// 을 지나고 나서 PostAccept 를 부른다. 그 사이에 주기 점검이 같은 슬롯을
	// 비었다고 읽으면 둘이 함께 건다. 같은 OverlappedEx 로 AcceptEx 가 두 번
	// 걸리고 WSASocket 도 두 번 불려 앞 핸들이 덮인다.
	// (실측: 창을 300ms 로 넓히면 슬롯 16개가 전부 32개로 걸렸다)
	//
	// 그래서 상태가 아니라 이 플래그가 유일한 근거다.
	volatile LONG m_slotOwned = 0;

public:
	bool Initialize(SESSION_ROLE sessionType, uint32_t sessionId) override;
	void ResetSession() override;
	void Finalize() override;

	bool OnAccept() override;
	bool OnConnect() override;
	bool OnDisconnect() override;

	// 슬롯을 가져간다. 이긴 쪽만 PostAccept 를 부를 수 있다.
	//
	// 소유권은 발행자에게서 그 발행의 완료를 처리하는 쪽으로 그대로 넘어간다.
	// 완료는 발행당 정확히 한 번이므로 인계에 별도 조치가 필요 없고,
	// 그래서 HandleAccept 는 이미 가진 소유권으로 곧바로 다시 걸면 된다.
	//
	// 놓는 시점은 슬롯이 실제로 비는 순간뿐이다 — PostAccept 가 실패했거나,
	// 취소된 AcceptEx 를 처리했거나, 종료 경로에서 다시 걸지 않을 때.
	bool TryAcquireSlot();
	void ReleaseSlot();

	OverlappedEx& GetAcceptOverlapped();
	char* GetAcceptBuffer();
};
