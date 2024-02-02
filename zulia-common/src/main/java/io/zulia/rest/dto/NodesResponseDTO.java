package io.zulia.rest.dto;

import java.util.List;

public class NodesResponseDTO {

	private List<NodeDTO> members;

	public NodesResponseDTO() {

	}

	public List<NodeDTO> getMembers() {
		return members;
	}

	public void setMembers(List<NodeDTO> members) {
		this.members = members;
	}

	@Override
	public String toString() {
		return "NodesResponse{" + "members=" + members + '}';
	}
}
