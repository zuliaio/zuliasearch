package io.zulia.server.index.replication;

import io.zulia.server.index.ShardReadManager;
import io.zulia.server.index.ZuliaShard;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.List;

import static io.zulia.server.index.replication.ReplicationUtil.PENDING_PREFIX;

public class ReplicaDirectoryApplier {

	private final static Logger LOG = LoggerFactory.getLogger(ReplicaDirectoryApplier.class);



	private final ZuliaShard replicaShard;

	private IndexOutput currentOutput;
	private Directory currentDirectory;
	private String currentLogicalName;
	private String currentWriteName;

	public ReplicaDirectoryApplier(ZuliaShard replicaShard) {
		if (replicaShard == null || replicaShard.getShardReadManager() == null) {
			throw new IllegalArgumentException("Replica shard must have a ShardReadManager (got " + replicaShard + ")");
		}
		this.replicaShard = replicaShard;
	}

	public void writeBytes(boolean taxonomy, String fileName, byte[] data, int offset, int length) throws IOException {
		ensureOpen(taxonomy, fileName);
		currentOutput.writeBytes(data, offset, length);
	}

	public void finishFile() throws IOException {
		if (currentOutput == null) {
			return;
		}
		try {
			currentOutput.close();
		}
		finally {
			currentOutput = null;
		}
		// every file lands via sync + atomic rename: writing a re-shipped data file under its real name
		// would delete a file the replica's live commit still references, and a concurrent reader refresh
		// in that window fails with NoSuchFileException. The rename replaces atomically, so a referenced
		// file is never absent, and the sync makes the data durable before any commit references it
		currentDirectory.sync(List.of(currentWriteName));
		currentDirectory.rename(currentWriteName, currentLogicalName);
		currentDirectory.syncMetaData();
		currentLogicalName = null;
		currentWriteName = null;
		currentDirectory = null;
	}

	// Throws on refresh failure so the caller does not advance the generation marker and old reader files stay valid.
	public void commit() throws IOException {
		try {
			replicaShard.refreshReaders();
		}
		catch (Exception e) {
			throw new IOException("refreshReaders failed for " + replicaShard + ":s" + replicaShard.getShardNumber(), e);
		}

		ShardReadManager readManager = replicaShard.getShardReadManager();
		if (readManager != null) {
			readManager.cleanupUnreferencedFiles();
		}
	}

	public void abort() {
		if (currentOutput != null) {
			try {
				currentOutput.close();
			}
			catch (Exception ignored) {
			}
			currentOutput = null;
		}
		currentLogicalName = null;
		currentWriteName = null;
		currentDirectory = null;
	}

	private void ensureOpen(boolean taxonomy, String fileName) throws IOException {
		if (currentOutput != null && fileName.equals(currentLogicalName)) {
			return;
		}
		if (currentOutput != null) {
			finishFile();
		}

		ShardReadManager readManager = replicaShard.getShardReadManager();
		Directory directory = taxonomy ? readManager.getTaxoDirectory() : readManager.getIndexDirectory();
		String writeName = PENDING_PREFIX + fileName;

		// createOutput is CREATE_NEW; clear any stale pending file with the same name.
		deleteIfExists(directory, writeName);

		currentDirectory = directory;
		currentLogicalName = fileName;
		currentWriteName = writeName;
		currentOutput = directory.createOutput(writeName, IOContext.DEFAULT);
	}

	private static void deleteIfExists(Directory directory, String fileName) {
		try {
			directory.deleteFile(fileName);
		}
		catch (NoSuchFileException e) {
		}
		catch (Exception e) {
			LOG.debug("Could not remove stale pending file {}: {}", fileName, e.getMessage());
		}
	}
}
