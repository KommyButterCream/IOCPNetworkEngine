#pragma once

#include <stdint.h>

#include "../Protocol/PacketID.h"
#include "../Job/JobDefs.h"

#ifdef BUILD_IOCP_ENGINE_DLL
#define IOCP_ENGINE_API __declspec(dllexport)
#else
#define IOCP_ENGINE_API __declspec(dllimport)
#endif

class IOCP_ENGINE_API PacketHandlerTable
{
protected:
	PacketHandlerFunc m_handlerTable[ToPacketID(PACKET_ID::MAX_PACKET_ID)];
	HandlerContext m_handlerContext = {};

public:
	PacketHandlerTable(const HandlerContext& context);
	virtual ~PacketHandlerTable();

public:
	bool Register(uint16_t packetId, PacketHandlerFunc handler);

	PacketHandlerFunc GetHandler(uint16_t packetId) const;
};