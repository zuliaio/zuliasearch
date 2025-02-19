package io.zulia.ai.dataset;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.Shape;
import ai.djl.training.dataset.RandomAccessDataset;
import ai.djl.training.dataset.Record;
import ai.djl.util.Progress;
import com.google.gson.Gson;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import io.zulia.ai.features.generator.ClassifierFeatureVector;
import io.zulia.ai.features.scaler.FeatureScaler;
import io.zulia.ai.features.stat.FeatureStat;
import io.zulia.ai.features.stat.FeatureStatGenerator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

public class DenseFeatureAndCategoryDataset extends RandomAccessDataset {
	
	private final List<ClassifierFeatureVector> classifierFeatureVectors;
	private final int categories;
	private final FeatureStat[] featureStats;
	private final IntIntMap categoryCountMap;
	private FeatureScaler featureScaler;
	
	public DenseFeatureAndCategoryDataset(Builder builder) {
		super(builder);
		this.classifierFeatureVectors = builder.classifierFeatureVectors;
		this.featureStats = builder.featureStats;
		this.categories = builder.getCategories();
		this.categoryCountMap = builder.categoryCountMap;
	}
	
	public int getNumberOfFeatures() {
		return this.featureStats.length;
	}
	
	public boolean isBinary() {
		return categories == 2;
	}
	
	public int getCategories() {
		return categories;
	}
	
	public IntIntMap getCategoryCountMap() {
		return categoryCountMap;
	}
	
	public FeatureStat[] getFeatureStats() {
		return featureStats;
	}
	
	public void setFeatureScaler(FeatureScaler featureScaler) {
		this.featureScaler = featureScaler;
	}
	
	@Override
	public Record get(NDManager ndManager, long index) throws IOException {
		ClassifierFeatureVector trainingFeatureVector = classifierFeatureVectors.get((int) index);
		float[] features = trainingFeatureVector.getFeatures();
		if (featureScaler != null) {
			features = featureScaler.scaleFeatures(features);
		}
		
		NDArray featureArray = ndManager.create(features, new Shape(features.length));
		float[] category;
		if (isBinary()) {
			category = new float[1];
			category[0] = (trainingFeatureVector.getCategory() == 1) ? 1 : 0;
		}
		else {
			category = new float[categories];
			category[trainingFeatureVector.getCategory()] = 1;
		}
		NDArray categoryArray = ndManager.create(category, new Shape(category.length));
		return new Record(new NDList(featureArray), new NDList(categoryArray));
	}
	
	@Override
	protected long availableSize() {
		return classifierFeatureVectors.size();
	}
	
	@Override
	public void prepare(Progress progress) {
	
	}
	
	public void shuffle() {
		Collections.shuffle(classifierFeatureVectors);
	}
	
	public static final class Builder extends BaseBuilder<Builder> {
		private static Gson gson = new Gson();
		
		private final List<ClassifierFeatureVector> classifierFeatureVectors;
		private String filename;
		private int categories;
		
		private FeatureStat[] featureStats;
		private IntIntMap categoryCountMap;
		
		public Builder() {
			super();
			this.classifierFeatureVectors = new ArrayList<>();
			this.categories = 2;
		}
		
		public String getFilename() {
			return filename;
		}
		
		public Builder setFilename(String filename) {
			this.filename = filename;
			return this;
		}
		
		public Builder setCategories(int categories) {
			if (categories < 2) {
				throw new IllegalArgumentException("Categories must be at least 2");
			}
			this.categories = categories;
			return this;
		}
		
		private int getCategories() {
			return categories;
		}
		
		@Override
		protected Builder self() {
			return this;
		}
		
		public DenseFeatureAndCategoryDataset build() throws IOException {
			
			int featureCount;
			try (Stream<String> lines = Files.lines(Paths.get(filename))) {
				String sample = lines.findFirst().get();
				ClassifierFeatureVector v = gson.fromJson(sample, ClassifierFeatureVector.class);
				featureCount = v.getFeatures().length;
			}
			FeatureStatGenerator featureStatGenerator = new FeatureStatGenerator(featureCount);
			
			this.categoryCountMap = HashIntIntMaps.newMutableMap();
			try (Stream<String> lines = Files.lines(Paths.get(filename))) {
				lines.forEach(s -> {
					ClassifierFeatureVector v = gson.fromJson(s, ClassifierFeatureVector.class);
					if (v.getCategory() >= categories) {
						throw new IllegalArgumentException(
										"Categories is set to " + categories + " but found category " + v.getCategory() + ". Expected values are between 0 and " + (categories - 1) + ".");
					}
					
					classifierFeatureVectors.add(v);
					featureStatGenerator.addExample(v.getFeatures());
					categoryCountMap.addValue(v.getCategory(), 1);
					
				});
			}
			
			this.featureStats = featureStatGenerator.computeFeatureStats();
			return new DenseFeatureAndCategoryDataset(this);
		}
	}
}
