package io.zulia.data.output;

import io.zulia.data.DataStreamMeta;

import java.io.IOException;
import java.io.OutputStream;

public interface DataOutputStream {

	OutputStream openOutputStream() throws IOException;

	DataStreamMeta getMeta();

}
