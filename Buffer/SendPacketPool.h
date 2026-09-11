#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>
#include <stdint.h>
#include "PreDefine.h"

using SendPacketReleaseFunc = void (*)(const void* packetData, void* context);

// 송신 대기 중인 패킷 하나를 가리키는 서술자. 내용을 복사하지 않고
// 패킷 풀의 주소를 그대로 들고 있는다.
//
// SLIST_ENTRY 를 상속하므로 반드시 첫 멤버 위치에 그 필드가 와야 하고,
// x64 SList 는 16바이트 정렬을 요구한다. alignas(64) 는 그보다 강하다.
struct alignas(64) SendPacketBuffer : public SLIST_ENTRY
{
	const char* packetData = nullptr;
	uint32_t packetSize = 0;
	SendPacketReleaseFunc releaseFunc = nullptr;
	void* releaseContext = nullptr;

	// 풀이 들고 있는가(0), 임대되어 있는가(1).
	//
	// 이중 반납을 막는 유일한 근거다. SList 는 같은 엔트리를 두 번 밀면
	// 자기참조 순환이 생기고, 그 뒤 Acquire 는 사용 중인 버퍼를 계속
	// 배포한다. ClientSessionPool 이 세션 노드에 대해 같은 문제로
	// SESSION_POOL_RELEASING 가드를 얻었는데 송신 버퍼에는 없었다.
	//
	// 송신 패킷의 반납 경로가 여럿이라(HandleSend, HandleSocketError,
	// ResetSession, Finalize) 실수의 여지가 실재한다.
	volatile LONG inUse = 0;

	inline void Reset() noexcept
	{
		packetData = nullptr;
		packetSize = 0;
		releaseFunc = nullptr;
		releaseContext = nullptr;
	}
};

class SendPacketPool
{
public:
	SendPacketPool();
	~SendPacketPool();

	bool Initialize(uint32_t blockCount = 512);
	void Finalize();

	SendPacketBuffer* Acquire();
	void Release(SendPacketBuffer* packetBuffer);

private:
	// SList 헤더만 캐시라인을 독점한다. 여러 스레드가 동시에 push/pop 하는
	// 유일한 대상이다.
	//
	// 예전에는 m_acqCount / m_relCount 도 각각 alignas(64) 였다. 그 둘은
	// 어디서도 읽지 않는 디버그 카운터였는데, Interlocked 연산은 부수효과가
	// 있어 컴파일러가 지우지 못한다. 송신 패킷 하나당 locked RMW 4개(획득 2,
	// 반납 2)를 태우고 sizeof 를 192바이트로 부풀리고 있었다.
	alignas(64) SLIST_HEADER m_sListHead;

	SendPacketBuffer* m_bufferPool = nullptr;   // 연속 메모리
	uint32_t m_blockCount = 0;
};
