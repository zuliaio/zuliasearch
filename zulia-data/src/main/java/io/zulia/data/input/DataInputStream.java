package io.zulia.data.input;

import io.zulia.data.common.DataStreamMeta;

import java.io.IOException;
import java.io.InputStream;

public interface DataInputStream {

	InputStream openInputStream() throws IOException;

	DataStreamMeta getMeta();

}
