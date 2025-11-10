package io.zulia.ai.nn.model.generics;

public record ClassifierEpochResult(int epoch, Float accuracy, Float f1, Float precision, Float recall, String modelSuffix) { }
