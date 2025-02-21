package io.zulia.ai.dataset.json;

import com.google.gson.Gson;
import io.zulia.ai.features.generator.ClassifierFeatureVector;
import io.zulia.ai.features.generator.TrainingFeatureVector;
import io.zulia.ai.nn.translator.DenseFeatureGenerator;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

public class DenseFeatureAndCategoryWriter<T> implements AutoCloseable{
	
	private final OutputStreamWriter featureWriter;
	private final DenseFeatureGenerator<T> featureGenerator;
	private final Gson gson;
	
	public DenseFeatureAndCategoryWriter(DenseFeatureGenerator<T> featureGenerator,  String featureFile) throws FileNotFoundException {
		this.featureWriter = new OutputStreamWriter(new FileOutputStream(featureFile), StandardCharsets.UTF_8);
		this.featureGenerator = featureGenerator;
		this.gson = new Gson();
	}
	
	public void writeExample(T example, int category) {
		ClassifierFeatureVector classifierFeatureVector = new ClassifierFeatureVector();
		classifierFeatureVector.setFeatures(featureGenerator.apply(example));
		classifierFeatureVector.setCategory(category);
		gson.toJson(classifierFeatureVector, featureWriter);
	}
	
	@Override
	public void close() throws Exception {
		this.featureWriter.close();
	}
}
