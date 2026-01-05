package io.zulia.fields;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import static io.zulia.message.ZuliaIndex.FacetAs;

public class FacetConfigBuilder {

	private boolean hierarchical;
	private FacetAs.DateHandling dateHandling;
	private String facetName;
	private boolean storeInOwnGroup;

	private Set<String> facetGroups;

	public static FacetConfigBuilder create() {
		return new FacetConfigBuilder();
	}

	public static FacetConfigBuilder createHierarchical() {
		FacetConfigBuilder facetConfigBuilder = new FacetConfigBuilder();
		facetConfigBuilder.hierarchical = true;
		return facetConfigBuilder;
	}

	private FacetConfigBuilder() {
	}

	public FacetConfigBuilder withDateHandling(FacetAs.DateHandling dateHandling) {
		this.dateHandling = dateHandling;
		return this;
	}

	public FacetConfigBuilder asFacet(String facetName) {
		this.facetName = facetName;
		return this;
	}

	public FacetConfigBuilder withGroup(String groupName) {
		this.facetGroups = Set.of(groupName);
		return this;
	}

	public FacetConfigBuilder withGroups(String... groupNames) {
		this.facetGroups = new LinkedHashSet<>(Arrays.asList(groupNames));
		return this;
	}

	public FacetConfigBuilder withGroups(Collection<String> groupNames) {
		this.facetGroups = new LinkedHashSet<>(groupNames);
		return this;
	}

	public FacetConfigBuilder storeInOwnGroup() {
		this.storeInOwnGroup = true;
		return this;
	}

	public FacetAs.Builder build() {
		FacetAs.Builder builder = FacetAs.newBuilder();
		builder.setHierarchical(hierarchical);
		builder.setStoreInOwnGroup(storeInOwnGroup);
		if (facetName != null) {
			builder.setFacetName(facetName);
		}
		if (dateHandling != null) {
			builder.setDateHandling(dateHandling);
		}

		if (facetGroups != null) {
			builder.addAllFacetGroups(facetGroups);
		}
		return builder;
	}
}
