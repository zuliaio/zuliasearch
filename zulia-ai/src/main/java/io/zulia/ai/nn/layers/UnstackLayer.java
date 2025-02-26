package io.zulia.ai.nn.layers;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.nn.LambdaBlock;

public class UnstackLayer extends LambdaBlock {
	public UnstackLayer() {
		super(ndList -> {
			NDArray output = ndList.getFirst();
			NDList outputs = output.split(2);
			NDArray embedding1 = outputs.get(0);
			NDArray embedding2 = outputs.get(1);
			return new NDList(embedding1, embedding2);
		});
	}
}
