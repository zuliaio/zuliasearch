package io.zulia.rest.dto;

import java.util.Set;

public class IndexMappingDTO {

	private String name;
	private int indexWeight;
	private Set<Integer> primary;
	private Set<Integer> replica;

	public IndexMappingDTO() {
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getIndexWeight() {
		return indexWeight;
	}

	public void setIndexWeight(int indexWeight) {
		this.indexWeight = indexWeight;
	}

	public Set<Integer> getPrimary() {
		return primary;
	}

	public void setPrimary(Set<Integer> primary) {
		this.primary = primary;
	}

	public Set<Integer> getReplica() {
		return replica;
	}

	public void setReplica(Set<Integer> replica) {
		this.replica = replica;
	}

	@Override
	public String toString() {
		return "IndexMapping{" + "name='" + name + '\'' + ", indexWeight=" + indexWeight + ", primary=" + primary + ", replica=" + replica + '}';
	}
}
