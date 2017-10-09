package org.vertx.kafka.util;

import java.net.InetAddress;
import java.util.HashMap;

import org.opennms.netmgt.dao.api.InterfaceToNodeCache;
import org.opennms.netmgt.dao.api.InterfaceToNodeMap;

public class MockInterfaceCacheDao implements InterfaceToNodeCache {
	private InterfaceToNodeMap m_knownips = new InterfaceToNodeMap();

	@Override
	public void dataSourceSync() {
	}

	@Override
	public int getNodeId(String location, InetAddress ipAddr) {
		if (ipAddr == null) {
			return -1;
		}
		return m_knownips.getNodeId(location, ipAddr);
	}

	@Override
	public int setNodeId(String location, InetAddress ipAddr, int nodeId) {
		if (ipAddr == null || nodeId == -1) {
			return -1;
		}
		return m_knownips.addManagedAddress(location, ipAddr, nodeId);
	}

	@Override
	public int removeNodeId(String location, InetAddress ipAddr) {
		if (ipAddr == null) {
			return -1;
		}
		return m_knownips.removeManagedAddress(location, ipAddr);
	}

	@Override
	public int size() {
		return m_knownips.size();
	}

	@Override
	public void clear() {
		m_knownips.setManagedAddresses(new HashMap<>());

	}

}
