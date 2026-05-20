package io.zulia.server.index.replication;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ReplicationUtil {

	private final static Logger LOG = LoggerFactory.getLogger(ReplicationUtil.class);

	private ReplicationUtil() {
	}

	public static boolean isSegmentsFile(String fileName) {
		return fileName != null && fileName.startsWith(IndexFileNames.SEGMENTS);
	}

	// Returns null if the file disappears or is unreadable; caller decides skip vs fail.
	public static ReplicaFileInfo readFileInfo(Directory directory, String fileName) {
		try {
			long length = directory.fileLength(fileName);
			String checksum = "";
			if (isSegmentsFile(fileName)) {
				try (IndexInput input = directory.openInput(fileName, IOContext.READONCE)) {
					checksum = Long.toHexString(CodecUtil.retrieveChecksum(input));
				}
			}
			return new ReplicaFileInfo(fileName, length, checksum);
		}
		catch (IOException e) {
			LOG.warn("Failed to read file info for {}: {}", fileName, e.getMessage());
			return null;
		}
	}
}
