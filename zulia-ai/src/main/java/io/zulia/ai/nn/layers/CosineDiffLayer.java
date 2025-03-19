package io.zulia.ai.nn.layers;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDArrays;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.AbstractBlock;
import ai.djl.training.ParameterStore;
import ai.djl.util.PairList;

public class CosineDiffLayer extends AbstractBlock {
    private final int[] AXES = {1};

    @Override
    protected NDList forwardInternal(
            ParameterStore parameterStore,
            NDList inputs,
            boolean training,
            PairList<String, Object> params) {
        NDList current = inputs;
        NDArray first = current.get(0);
        NDArray second = current.get(1);

        NDArray dot = NDArrays.mul(first, second).sum(AXES);

        NDArray norm1 = first.norm(AXES, false);
        NDArray norm2 = second.norm(AXES, false);

        NDArray result = dot.div(norm1.mul(norm2)).reshape(first.getShape().getShape()[0], 1);

        return new NDList(result);
    }

    @Override
    public Shape[] getOutputShapes(Shape[] inputs) {
        return new Shape[]{new Shape(inputs[0].getShape()[0], 1)};
    }
}