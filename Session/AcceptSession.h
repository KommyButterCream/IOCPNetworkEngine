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

public:
	bool Initialize(SESSION_ROLE sessionType, uint32_t sessionId) override;
	void ResetSession() override;
	void Finalize() override;

	bool OnAccept() override;
	bool OnConnect() override;
	bool OnDisconnect() override;

	OverlappedEx& GetAcceptOverlapped();
	char* GetAcceptBuffer();
};
