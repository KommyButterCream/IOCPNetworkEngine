#pragma once

class ISession;

struct SessionNode
{
	ISession* session = nullptr; // 이 노드가 가리키는 Session 포인터
	SessionNode* nextNode = nullptr;    // 프리 리스트의 다음 노드

	SessionNode() = default;

	~SessionNode()
	{
		session = nullptr;
		nextNode = nullptr;
	}
};
