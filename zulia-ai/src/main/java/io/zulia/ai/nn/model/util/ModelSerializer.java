package io.zulia.ai.nn.model.util;

import ai.djl.MalformedModelException;
import ai.djl.Model;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Block;
import ai.djl.util.Pair;
import ai.djl.util.PairList;
import io.zulia.ai.nn.config.FullyConnectedConfiguration;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

public class ModelSerializer {

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

	// Lifted from inside djl, which works only on filesystem
	public static byte[] serializeModel(Model model) throws IOException {
		try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
			try (DataOutputStream dos = new DataOutputStream(byteArrayOutputStream)) {
				dos.writeBytes("DJL@");
				dos.writeInt(1);
				dos.writeUTF(model.getName());
				dos.writeUTF(model.getDataType().name());
				PairList<String, Shape> inputData = model.getBlock().describeInput();
				dos.writeInt(inputData.size());
				// Put in the descriptors
				for (Pair<String, Shape> desc : inputData) {
					String name = desc.getKey();
					dos.writeUTF(name != null ? name : "");
					dos.write(desc.getValue().getEncoded());
				}

				dos.writeInt(model.getProperties().size());
				for (Map.Entry<String, String> entry : model.getProperties().entrySet()) {
					dos.writeUTF(entry.getKey());
					dos.writeUTF(entry.getValue());
				}

				model.getBlock().saveParameters(dos);
			}

			return byteArrayOutputStream.toByteArray(); // to a buffer we can use anywhere
		}
	}

	// Wrapper around build in method for loading a model
	public static Model deserializeModelFromBuffer(byte[] buffer, String modelName) throws IOException, MalformedModelException {
		try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(buffer)) {
			try (DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream)) {
				Model newModel = Model.newInstance(modelName);
				newModel.load(dataInputStream);
				return newModel;
			}
		}
	}
}
