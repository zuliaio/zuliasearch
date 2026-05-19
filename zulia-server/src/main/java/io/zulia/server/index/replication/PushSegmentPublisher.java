package io.zulia.server.index.replication;

import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaServiceOuterClass.GetSegmentFileInfoResponse;
import io.zulia.message.ZuliaServiceOuterClass.SegmentFileInfo;
import io.zulia.server.connection.client.InternalClient;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PushSegmentPublisher implements SegmentPublisher {

	private final static Logger LOG = LoggerFactory.getLogger(PushSegmentPublisher.class);

	private final InternalClient internalClient;
	private final int responseTimeoutMinutes;
	private final ReplicationRateLimiter rateLimiter;

	public PushSegmentPublisher(InternalClient internalClient, int responseTimeoutMinutes, ReplicationRateLimiter rateLimiter) {
		this.internalClient = internalClient;
		this.responseTimeoutMinutes = responseTimeoutMinutes;
		this.rateLimiter = rateLimiter;
	}

	@Override
	public void publish(PublishRequest request) throws Exception {
		Node replicaNode = request.replicaNode();
		int shardNumber = request.shardNumber();
		String indexName = request.indexName();
		boolean taxonomy = request.taxonomy();
		Directory sourceDirectory = request.sourceDirectory();
		long generation = request.generation();

		DirectSegmentReplicator replicator = new DirectSegmentReplicator(internalClient, replicaNode, responseTimeoutMinutes, rateLimiter);

		GetSegmentFileInfoResponse replicaInfo = replicator.getSegmentFileInfo(indexName, shardNumber, taxonomy);
		checkLuceneVersion(replicaInfo, replicaNode);
		Map<String, SegmentFileInfo> replicaFileMap = indexByName(replicaInfo);

		Map<String, ReplicaFileInfo> sourceInfos = readSourceInfos(sourceDirectory, request.sourceFiles(), indexName, shardNumber);

		// Segment names are per-index, so equal length does not imply equal content; if segments_N differs, ship all.
		boolean segmentsHeaderDiffers = segmentsHeaderDiffers(sourceInfos, replicaFileMap);

		List<SegmentFileInfo> filesToSend = new ArrayList<>(sourceInfos.size());
		for (Map.Entry<String, ReplicaFileInfo> entry : sourceInfos.entrySet()) {
			String name = entry.getKey();
			ReplicaFileInfo info = entry.getValue();
			SegmentFileInfo existing = replicaFileMap.get(name);
			boolean shouldSend = segmentsHeaderDiffers || existing == null || existing.getLength() != info.length();
			if (shouldSend) {
				filesToSend.add(SegmentFileInfo.newBuilder().setFileName(name).setLength(info.length()).setChecksum(info.checksum()).build());
			}
		}

		if (filesToSend.isEmpty()) {
			LOG.debug("No files to replicate for {}:s{} {} to {}:{}", indexName, shardNumber, taxonomy ? "taxonomy" : "index", replicaNode.getServerAddress(),
					replicaNode.getServicePort());
			return;
		}

		// segments_N must be sent last, so a mid-stream refresh on the replica can't see a commit pointing at
		// files it doesn't have yet. SegmentInfos.files() returns a HashSet, so it needs a sort to put it last
		filesToSend.sort(Comparator.comparing(info -> ReplicationUtil.isSegmentsFile(info.getFileName())));

		LOG.info("Replicating {} files for {}:s{} {} to {}:{}", filesToSend.size(), indexName, shardNumber, taxonomy ? "taxonomy" : "index",
				replicaNode.getServerAddress(), replicaNode.getServicePort());

		replicator.sendSegmentFiles(indexName, shardNumber, taxonomy, filesToSend, sourceDirectory, generation);

		LOG.info("Completed replication for {}:s{} {} to {}:{} at generation {}", indexName, shardNumber, taxonomy ? "taxonomy" : "index",
				replicaNode.getServerAddress(), replicaNode.getServicePort(), generation);
	}

	// Replica Lucene must be >= primary; Lucene is not forward-compatible. Fail fast before bytes hit disk.
	private static void checkLuceneVersion(GetSegmentFileInfoResponse replicaInfo, Node replicaNode) throws ReplicationStreamException {
		Version primaryVersion = Version.LATEST;
		String replicaVersionStr = replicaInfo.getLuceneVersion();
		String replicaAddress = replicaNode.getServerAddress() + ":" + replicaNode.getServicePort();
		if (replicaVersionStr.isEmpty()) {
			throw new ReplicationStreamException("Replica " + replicaAddress + " did not report a Lucene version; refusing to replicate");
		}
		Version replicaVersion;
		try {
			replicaVersion = Version.parse(replicaVersionStr);
		}
		catch (ParseException e) {
			throw new ReplicationStreamException("Replica " + replicaAddress + " reported unparseable Lucene version '" + replicaVersionStr + "'");
		}
		if (!replicaVersion.onOrAfter(primaryVersion)) {
			throw new ReplicationStreamException(
					"Replica " + replicaAddress + " is on Lucene " + replicaVersionStr + " but primary is on " + primaryVersion
							+ "; replica must be at or newer than primary (upgrade replicas first during a rolling upgrade)");
		}
	}

	private static Map<String, SegmentFileInfo> indexByName(GetSegmentFileInfoResponse replicaInfo) {
		Map<String, SegmentFileInfo> result = new LinkedHashMap<>(Math.max(16, replicaInfo.getFilesCount() * 2));
		for (SegmentFileInfo fileInfo : replicaInfo.getFilesList()) {
			result.put(fileInfo.getFileName(), fileInfo);
		}
		return result;
	}

	private static Map<String, ReplicaFileInfo> readSourceInfos(Directory sourceDirectory, Collection<String> sourceFiles, String indexName, int shardNumber)
			throws IOException {
		Map<String, ReplicaFileInfo> result = new LinkedHashMap<>(Math.max(16, sourceFiles.size() * 2));
		for (String sourceFile : sourceFiles) {
			ReplicaFileInfo info = ReplicationUtil.readFileInfo(sourceDirectory, sourceFile);
			if (info == null) {
				throw new IOException("Source file " + sourceFile + " is unreadable from a held snapshot for " + indexName + ":s" + shardNumber);
			}
			result.put(sourceFile, info);
		}
		return result;
	}

	private static boolean segmentsHeaderDiffers(Map<String, ReplicaFileInfo> sourceInfos, Map<String, SegmentFileInfo> replicaFileMap) {
		for (Map.Entry<String, ReplicaFileInfo> entry : sourceInfos.entrySet()) {
			if (!ReplicationUtil.isSegmentsFile(entry.getKey())) {
				continue;
			}
			ReplicaFileInfo info = entry.getValue();
			SegmentFileInfo existing = replicaFileMap.get(entry.getKey());
			if (existing == null || existing.getLength() != info.length() || !existing.getChecksum().equals(info.checksum())) {
				return true;
			}
		}
		return false;
	}
}
