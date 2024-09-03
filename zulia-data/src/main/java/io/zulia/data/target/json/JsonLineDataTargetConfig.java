package io.zulia.data.target.json;

import io.zulia.data.output.DataOutputStream;

public class JsonLineDataTargetConfig {

	private ObjectSerializer objectSerializer;

	public static JsonLineDataTargetConfig from(DataOutputStream dataStream) {
		return new JsonLineDataTargetConfig(dataStream);
	}

	private final DataOutputStream dataStream;

	private JsonLineDataTargetConfig(DataOutputStream dataStream) {
		this.dataStream = dataStream;
		this.objectSerializer = new GsonObjectSerializer();
	}

	public JsonLineDataTargetConfig withObjectSerializer(ObjectSerializer objectSerializer) {
		this.objectSerializer = objectSerializer;
		return this;
	}

	public ObjectSerializer getObjectSerializer() {
		return objectSerializer;
	}

	public DataOutputStream getDataStream() {
		return dataStream;
	}

}
