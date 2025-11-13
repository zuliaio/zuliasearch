package io.zulia.server.serde;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public interface ProtoIgnoredGetters {
	@JsonIgnore
	com.google.protobuf.Descriptors.Descriptor getDescriptorForType();

	@JsonIgnore
	com.google.protobuf.Parser<?> getParserForType();

	@JsonIgnore
	com.google.protobuf.Message getDefaultInstanceForType();

	@JsonIgnore
	com.google.protobuf.UnknownFieldSet getUnknownFields();

	@JsonIgnore
	java.util.Map<com.google.protobuf.Descriptors.FieldDescriptor, Object> getAllFields();

	@JsonIgnore
	int getSerializedSize();

	@JsonIgnore
	String getInitializationErrorString();

	@JsonIgnore
	boolean isInitialized();

}