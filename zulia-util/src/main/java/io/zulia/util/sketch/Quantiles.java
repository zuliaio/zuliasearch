package io.zulia.util.sketch;

import java.util.function.DoubleConsumer;


public sealed interface Quantiles extends DoubleConsumer permits QuantileSketch, SummarizedQuantileSketch {

	QuantileSketch.Mapping mapping();

	double relativeAccuracy();

	long count();

	boolean isEmpty();

	double quantile(double quantile);

	double[] quantiles(double[] ascending);
}
