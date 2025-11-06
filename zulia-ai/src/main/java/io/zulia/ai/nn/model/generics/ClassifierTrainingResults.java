package io.zulia.ai.nn.model.generics;

import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.List;

public class ClassifierTrainingResults {
	private final String uuid;
	private final Gson gson = new Gson();

	private final List<ClassifierEpochResult> classifierEpochResults;

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

}
