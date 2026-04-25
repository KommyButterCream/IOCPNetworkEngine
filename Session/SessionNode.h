#pragma once

class ISession;

struct SessionNode
{
	ISession* session = nullptr; // ���� Session ������
	SessionNode* nextNode = nullptr;    // ���� ����Ʈ ����

	SessionNode() = default;

	~SessionNode()
	{
		session = nullptr;
		nextNode = nullptr;
	}
};
