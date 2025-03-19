package io.zulia.ai.features.stat;

import java.io.*;

public class FeatureStatPersistence {
	
	public static void save(FeatureStat[] featureStats, OutputStream outputStream) throws IOException {
		try (ObjectOutput output = new ObjectOutputStream(outputStream)) {
			
			output.writeInt(featureStats.length);
			
			for (FeatureStat featureStat : featureStats) {
				output.writeDouble(featureStat.getAvg());
				output.writeDouble(featureStat.getMin());
				output.writeDouble(featureStat.getP05());
				output.writeDouble(featureStat.getP10());
				output.writeDouble(featureStat.getP25());
				output.writeDouble(featureStat.getP50());
				output.writeDouble(featureStat.getP75());
				output.writeDouble(featureStat.getP90());
				output.writeDouble(featureStat.getP95());
				output.writeDouble(featureStat.getMax());
			}
			
		}
	}
	
	public static FeatureStat[] load(InputStream inputStream) throws IOException {
		try (ObjectInputStream objectInputStream = new ObjectInputStream(inputStream)) {
			int numberOfFeatures = objectInputStream.readInt();
			FeatureStat[] featureStats = new FeatureStat[numberOfFeatures];
			
			for (int i = 0; i < numberOfFeatures; i++) {
				FeatureStat featureStat = new FeatureStat();
				featureStat.setAvg(objectInputStream.readDouble());
				featureStat.setMin(objectInputStream.readDouble());
				featureStat.setP05(objectInputStream.readDouble());
				featureStat.setP10(objectInputStream.readDouble());
				featureStat.setP25(objectInputStream.readDouble());
				featureStat.setP50(objectInputStream.readDouble());
				featureStat.setP75(objectInputStream.readDouble());
				featureStat.setP90(objectInputStream.readDouble());
				featureStat.setP95(objectInputStream.readDouble());
				featureStats[i] = featureStat;
			}
			return featureStats;
		}
	}
}
