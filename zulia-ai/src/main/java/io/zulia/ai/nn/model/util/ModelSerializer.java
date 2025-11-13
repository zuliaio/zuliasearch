package io.zulia.ai.nn.model.util;

import ai.djl.MalformedModelException;
import ai.djl.Model;
import ai.djl.nn.Block;
import io.zulia.ai.features.scaler.FeatureScaler;
import io.zulia.ai.nn.config.FullyConnectedConfiguration;
import org.apache.commons.lang3.SerializationUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ModelSerializer {

	private static final int VERSION = 1;

	/**
	 * Used to take a snapshot of the training settings for rollback
	 *
	 * @param trainingNetwork network block being frozen
	 * @return buffer snapshot of network history
	 */
	public static byte[] serializeNetworkInMemory(Block trainingNetwork) throws IOException {
		byte[] inMemoryData;
		try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
			try (DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
				trainingNetwork.saveParameters(dataOutputStream);
			}

			byteArrayOutputStream.flush();
			inMemoryData = byteArrayOutputStream.toByteArray();
		}
		return inMemoryData;
	}

	/**
	 * Used to convert a snapshotted buffer back into a useable model (for return to user)
	 *
	 * @param inMemoryData buffer of a network to be read
	 * @param model        to put this network into
	 */
	public static void deserializeParametersFromBuffer(byte[] inMemoryData, Model model, Block evaluationNetwork) throws IOException, MalformedModelException {
		// Read from memory buffer into output block
		try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(inMemoryData)) {
			try (DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream)) {
				evaluationNetwork.loadParameters(model.getNDManager(), dataInputStream);
			}
		}

		model.setBlock(evaluationNetwork);
	}

	/**
	 * Used to convert a snapshotted buffer back into a useable model (for return to user)
	 *
	 * @param inMemoryData buffer of a network to be read
	 * @param model        to put this network into
	 */
	public static void deserializeParametersFromBuffer(byte[] inMemoryData, Model model, FullyConnectedConfiguration fullyConnectedConfiguration)
			throws IOException, MalformedModelException {
		deserializeParametersFromBuffer(inMemoryData, model, fullyConnectedConfiguration.getEvaluationNetwork());
	}

	/**
	 * Serialize a model for later loading / reading
	 *
	 * @param model         model to store
	 * @param configuration configuration which will be needed to set up blocks
	 * @return buffer of serialized model
	 */
	public static byte[] serializeModel(Model model, FullyConnectedConfiguration configuration) throws IOException {
		return serializeModel(model, configuration, null); // simpler version
	}

	/**
	 * Serialize a model for later loading / reading
	 *
	 * @param model         model to store
	 * @param configuration configuration which will be needed to set up blocks
	 * @param featureScaler scaler needed to make a predictor
	 * @return buffer of serialized model
	 */
	public static byte[] serializeModel(Model model, FullyConnectedConfiguration configuration, FeatureScaler featureScaler) throws IOException {
		try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
			try (DataOutputStream dos = new DataOutputStream(byteArrayOutputStream)) {
				dos.writeUTF("OPA_DJL");
				dos.writeInt(VERSION);
				// Write out the configuration
				byte[] config = SerializationUtils.serialize(configuration);
				dos.writeInt(config.length);
				dos.write(config);

				// Write out the name
				dos.writeUTF(model.getName());

				// Write out the parameters
				model.getBlock().saveParameters(dos);

				// Optionally write the feature scaler
				if (featureScaler != null) {
					byte[] scaler = SerializationUtils.serialize(featureScaler);
					dos.writeInt(scaler.length);
					dos.write(scaler);
				}
			}
			return byteArrayOutputStream.toByteArray();
		}
	}

	/**
	 * Deserialize a functional model from the buffer
	 *
	 * @param buffer serialized model
	 * @return Model with parameters configured
	 */
	public static Model deserializeModelFromBuffer(byte[] buffer) throws IOException, MalformedModelException {
		return deserializeFullModelFromBuffer(buffer).model();
	}

	/**
	 * Deserialize a functional model from the buffer
	 *
	 * @param buffer serialized model
	 * @return Model with parameters configured and a feature scaler to use with it
	 */
	public static FullModel deserializeFullModelFromBuffer(byte[] buffer) throws IOException, MalformedModelException {
		try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(buffer)) {
			try (DataInputStream dis = new DataInputStream(byteArrayInputStream)) {
				String header = dis.readUTF();
				assert header.equals("OPA_DJL");

				int version = dis.readInt();
				if (version != VERSION) {
					throw new RuntimeException("Version mismatch");
				}

				int blockSize = dis.readInt();
				byte[] serializedSettings = dis.readNBytes(blockSize);
				FullyConnectedConfiguration fcc = SerializationUtils.deserialize(serializedSettings);

				String name = dis.readUTF();
				Model model = Model.newInstance(name);

				// Read parameters
				Block newBlock = fcc.getEvaluationNetwork();
				newBlock.loadParameters(model.getNDManager(), dis);

				// Build your model
				model.setBlock(newBlock);

				// Check for the next part
				FeatureScaler featureScaler = null;
				if (dis.available() > 0) {
					int scalerSize = dis.readInt();
					if (scalerSize > 0) {
						byte[] serialized = dis.readNBytes(scalerSize);
						featureScaler = SerializationUtils.deserialize(serialized);
					}
				}

				// Send back the carrier
				return new FullModel(model, featureScaler);
			}
		}
	}

	public record FullModel(Model model, FeatureScaler scaler) implements AutoCloseable {
		@Override
		public void close() throws Exception {
			model.close();
		}
	}
}
