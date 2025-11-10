package io.zulia.ai.nn.model.generics;

import ai.djl.Model;
import io.zulia.ai.features.scaler.FeatureScaler;

import java.util.ArrayList;
import java.util.List;

public class ClassifierTrainingResults {
	private final String uuid;
	private final List<ClassifierEpochResult> classifierEpochResults;
	// Results could come with a model, if desired
	private Model resultModel;
	private FeatureScaler resultFeatureScaler;

	public ClassifierTrainingResults(String uuid) {
		this.uuid = uuid;
		this.classifierEpochResults = new ArrayList<>();
	}

	public String getUuid() {
		return uuid;
	}

	public void addEpochResult(ClassifierEpochResult epochResults) {
		classifierEpochResults.add(epochResults);
	}

	public List<ClassifierEpochResult> getClassifierEpochResults() {
		return classifierEpochResults;
	}

	public ClassifierEpochResult getBestEpoch() {
		ClassifierEpochResult f1 = getBestF1Epoch();
		if (f1 == null) {
			return getBestAccuracyEpoch();
		}
		else {
			return f1;
		}
	}

	public ClassifierEpochResult getBestF1Epoch() {
		ClassifierEpochResult bestEpochResult = null;
		for (ClassifierEpochResult epochResult : classifierEpochResults) {
			if (epochResult.f1() == null) {
				continue;
			}
			if (bestEpochResult == null || epochResult.f1() > bestEpochResult.f1()) {
				bestEpochResult = epochResult;
			}
		}

		return bestEpochResult;
	}

	public ClassifierEpochResult getBestAccuracyEpoch() {
		ClassifierEpochResult bestEpochResult = null;
		for (ClassifierEpochResult epochResult : classifierEpochResults) {
			if (epochResult.accuracy() == null) {
				continue;
			}
			if (bestEpochResult == null || epochResult.accuracy() > bestEpochResult.accuracy()) {
				bestEpochResult = epochResult;
			}
		}

		return bestEpochResult;
	}

	public FeatureScaler getResultFeatureScaler() {
		return resultFeatureScaler;
	}

	public void setResultFeatureScaler(FeatureScaler resultFeatureScaler) {
		this.resultFeatureScaler = resultFeatureScaler;
	}

	public Model getResultModel() {
		return resultModel;
	}

	public void setResultModel(Model resultModel) {
		this.resultModel = resultModel;
	}
}
