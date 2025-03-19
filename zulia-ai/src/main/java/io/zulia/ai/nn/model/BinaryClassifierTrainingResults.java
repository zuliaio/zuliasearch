package io.zulia.ai.nn.model;

import java.util.ArrayList;
import java.util.List;

public class BinaryClassifierTrainingResults {
	private final String uuid;
	
	private final List<BinaryClassifierEpochResult> binaryClassifierEpochResults;
	
	public BinaryClassifierTrainingResults(String uuid) {
		this.uuid = uuid;
		this.binaryClassifierEpochResults = new ArrayList<>();
	}
	
	public String getUuid() {
		return uuid;
	}
	
	public void addEpochResult(BinaryClassifierEpochResult epochResults) {
		binaryClassifierEpochResults.add(epochResults);
	}
	
	public List<BinaryClassifierEpochResult> getBinaryClassifierEpochResults() {
		return binaryClassifierEpochResults;
	}
	
	public BinaryClassifierEpochResult getBestF1Epoch() {
		
		BinaryClassifierEpochResult bestEpochResult = null;
		for (BinaryClassifierEpochResult epochResult : binaryClassifierEpochResults) {
			if (bestEpochResult == null || epochResult.f1() > bestEpochResult.f1()) {
				bestEpochResult = epochResult;
			}
		}
		
		return bestEpochResult;
	}
	

}
