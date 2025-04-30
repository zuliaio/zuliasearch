package io.zulia.ai.nn.layers;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.nn.LambdaBlock;

public class StackLayer extends LambdaBlock {
	public StackLayer() {
		super(ndList -> {
			NDArray ndArray1 = ndList.get(0);
			NDArray ndArray2 = ndList.get(1);
			NDArray combined = ndArray1.concat(ndArray2, 0);
			return new NDList(combined);
		});
	}
}
