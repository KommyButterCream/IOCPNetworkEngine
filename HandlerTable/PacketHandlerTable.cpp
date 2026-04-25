#include "PacketHandlerTable.h"
#include <memory.h> // for memset

PacketHandlerTable::PacketHandlerTable(const HandlerContext& context)
{
	m_handlerContext = context;

	memset(m_handlerTable, 0, sizeof(m_handlerTable));
}

PacketHandlerTable::~PacketHandlerTable()
{
	memset(m_handlerTable, 0, sizeof(m_handlerTable));
}

bool PacketHandlerTable::Register(uint16_t packetId, PacketHandlerFunc handler)
{
	constexpr uint16_t maxPacketId = ToPacketID(PACKET_ID::MAX_PACKET_ID);
	if (packetId >= maxPacketId)
		return false;

	if (!handler)
		return false;

	m_handlerTable[packetId] = handler;

	return true;
}

PacketHandlerFunc PacketHandlerTable::GetHandler(uint16_t packetId) const
{
	constexpr uint16_t maxPacketId = ToPacketID(PACKET_ID::MAX_PACKET_ID);
	if (packetId >= maxPacketId)
		return nullptr;

	return m_handlerTable[packetId];
}