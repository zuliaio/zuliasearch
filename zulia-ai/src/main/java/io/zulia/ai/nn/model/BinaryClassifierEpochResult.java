package io.zulia.ai.nn.model;

public record BinaryClassifierEpochResult(int epoch, float f1, float precision, float recall, String modelSuffix) {
}
