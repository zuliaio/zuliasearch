package io.zulia.server;

import io.zulia.message.ZuliaBase.Member;

import java.util.List;

public interface MembershipConfig {

	/**
	 *
	 * @return All members registered with a cluster
	 */
	List<Member> getMembers();

	/**
	 * Register a new member with a cluster
	 * Keyed on server name and hazelcast port
	 *
	 * @param member - new member to register with the cluster or member to update
	 */
	void registerMember(Member member);

	/**
	 * Removed a member from the cluster
	 * Keyed on server name and hazelcast port
	 * @param member - member to remove from the cluster
	 */
	void removeMember(Member member);

}
