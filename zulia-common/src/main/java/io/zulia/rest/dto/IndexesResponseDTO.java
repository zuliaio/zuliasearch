package io.zulia.rest.dto;

import java.util.List;

public class IndexesResponseDTO {

	private List<String> indexes;

	public IndexesResponseDTO() {

	}

	public List<String> getIndexes() {
		return indexes;
	}

	public void setIndexes(List<String> indexes) {
		this.indexes = indexes;
	}

	@Override
	public String toString() {
		return "IndexesResponse{" + "indexes=" + indexes + '}';
	}
}
