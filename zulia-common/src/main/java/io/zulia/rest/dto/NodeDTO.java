package io.zulia.rest.dto;

import java.util.List;

public class NodeDTO {

	private String serverAddress;
	private int servicePort;
	private int restPort;
	private long heartbeat;
	private List<IndexMappingDTO> indexMappings;

	public NodeDTO() {
	}

	public String getServerAddress() {
		return serverAddress;
	}

	public void setServerAddress(String serverAddress) {
		this.serverAddress = serverAddress;
	}

	public int getServicePort() {
		return servicePort;
	}

	public void setServicePort(int servicePort) {
		this.servicePort = servicePort;
	}

	public int getRestPort() {
		return restPort;
	}

	public void setRestPort(int restPort) {
		this.restPort = restPort;
	}

	public long getHeartbeat() {
		return heartbeat;
	}

	public void setHeartbeat(long heartbeat) {
		this.heartbeat = heartbeat;
	}

	public List<IndexMappingDTO> getIndexMappings() {
		return indexMappings;
	}

	public void setIndexMappings(List<IndexMappingDTO> indexMappings) {
		this.indexMappings = indexMappings;
	}

	@Override
	public String toString() {
		return "NodesResponse{" + "serverAddress='" + serverAddress + '\'' + ", servicePort=" + servicePort + ", restPort=" + restPort + ", heartbeat="
				+ heartbeat + ", indexMappings=" + indexMappings + '}';
	}
}
