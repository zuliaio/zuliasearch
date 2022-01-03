package io.zulia.client.result;

import static io.zulia.message.ZuliaServiceOuterClass.DeleteIndexAliasResponse;

public class DeleteIndexAliasResult extends Result {

	@SuppressWarnings("unused")
	private DeleteIndexAliasResponse deleteIndexAliasResponse;

	public DeleteIndexAliasResult(DeleteIndexAliasResponse deleteIndexAliasResponse) {
		this.deleteIndexAliasResponse = deleteIndexAliasResponse;
	}

}
