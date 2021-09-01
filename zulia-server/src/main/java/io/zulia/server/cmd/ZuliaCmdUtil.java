package io.zulia.server.cmd;

import com.google.common.base.Charsets;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.zulia.client.command.Fetch;
import io.zulia.client.command.FetchAllAssociated;
import io.zulia.client.command.Query;
import io.zulia.client.command.Store;
import io.zulia.client.command.StoreLargeAssociated;
import io.zulia.client.pool.WorkPool;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.AssociatedResult;
import io.zulia.client.result.FetchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.server.config.NodeService;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.config.cluster.MongoAuth;
import io.zulia.server.config.cluster.MongoNodeService;
import io.zulia.server.config.cluster.MongoServer;
import io.zulia.server.config.single.SingleNodeService;
import io.zulia.server.util.MongoProvider;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import static io.zulia.message.ZuliaQuery.FetchType.META;

public class ZuliaCmdUtil {

	private static final Logger LOG = Logger.getLogger(ZuliaCmdUtil.class.getSimpleName());

	public static NodeService getNodeService(ZuliaConfig zuliaConfig) {
		if (zuliaConfig.isCluster()) {
			List<MongoServer> mongoServers = zuliaConfig.getMongoServers();

			List<ServerAddress> serverAddressList = new ArrayList<>();

			for (MongoServer mongoServer : mongoServers) {
				LOG.info("Added Mongo Server: " + mongoServer);
				serverAddressList.add(new ServerAddress(mongoServer.getHostname(), mongoServer.getPort()));
			}

			MongoClientSettings.Builder mongoBuilder = MongoClientSettings.builder().applyToClusterSettings(builder -> builder.hosts(serverAddressList));

			MongoAuth mongoAuth = zuliaConfig.getMongoAuth();
			if (mongoAuth != null) {
				mongoBuilder.credential(
						MongoCredential.createCredential(mongoAuth.getUsername(), mongoAuth.getDatabase(), mongoAuth.getPassword().toCharArray()));
			}

			MongoClient mongoClient = MongoClients.create(mongoBuilder.build());

			MongoProvider.setMongoClient(mongoClient);
			LOG.info("Created Mongo Client: " + MongoProvider.getMongoClient());

			return new MongoNodeService(MongoProvider.getMongoClient(), zuliaConfig.getClusterName());
		}
		else {
			return new SingleNodeService(zuliaConfig);
		}
	}

	public static void writeOutput(String recordsFilename, String index, String q, int rows, ZuliaWorkPool workPool, AtomicInteger count, String idField,
			Set<String> uniqueIds, boolean sortById) throws Exception {
		try (FileWriter fileWriter = new FileWriter(new File(recordsFilename), Charsets.UTF_8)) {

			Query zuliaQuery = new io.zulia.client.command.Query(index, q, rows);
			if (sortById) {
				zuliaQuery.addFieldSort(idField);
			}

			try {
				workPool.queryAll(zuliaQuery, queryResult -> {

					long totalHits = queryResult.getTotalHits();
					LOG.info("Found <" + totalHits + "> for index <" + index + ">");

					queryResult.getDocuments().forEach(doc -> {
						try {
							if (uniqueIds != null) {
								uniqueIds.add(doc.getString(idField));
							}
							fileWriter.write(doc.toJson());
							fileWriter.write(System.lineSeparator());

							if (count.incrementAndGet() % 1000 == 0) {
								LOG.info("So far written <" + count + "> of <" + totalHits + ">");
							}

						}
						catch (IOException e) {
							LOG.log(Level.SEVERE, "Could not write record <" + doc + "> for index <" + index + ">", e);
						}
						catch (Throwable e) {
							LOG.log(Level.SEVERE, "Could not write output for index <" + index + ">", e);
						}

					});
				});
			}
			catch (Throwable t) {
				LOG.log(Level.SEVERE, "Query failed for index <" + index + ">", t);
			}

		}
		catch (Throwable e) {
			LOG.log(Level.SEVERE, "Could not write output for index <" + index + ">", e);
			throw e;
		}
	}

	public static void index(String inputDir, String recordsFilename, String idField, String index, ZuliaWorkPool workPool, AtomicInteger count,
			Integer threads, Boolean skipExistingFiles) throws Exception {
		WorkPool threadPool = new WorkPool(threads);
		try (BufferedReader b = new BufferedReader(new FileReader(recordsFilename))) {
			String line;
			while ((line = b.readLine()) != null) {
				final String record = line;
				threadPool.executeAsync((Callable<Void>) () -> {
					try {
						Document document = Document.parse(record);
						String id = null;
						if (idField != null) {
							id = document.getString(idField);
						}
						if (id == null) {
							// fall through to just "id"
							id = document.getString("id");
						}

						if (id == null) {
							// if still null, throw exception
							throw new RuntimeException("No id for record: " + document.toJson());
						}

						document.put("indexTime", new Date());

						Store store = new Store(id, index);
						store.setResultDocument(new ResultDocBuilder().setDocument(document));
						workPool.store(store);

						if (Files.exists(Paths.get(inputDir + File.separator + id.replaceAll("/", "_") + ".zip"))) {

							try (ZipArchiveInputStream inputStream = new ZipArchiveInputStream(
									new FileInputStream(Paths.get(inputDir + File.separator + id.replaceAll("/", "_") + ".zip").toFile()))) {
								ZipArchiveEntry zipEntry;
								while ((zipEntry = inputStream.getNextZipEntry()) != null) {
									try {
										if (skipExistingFiles) {
											if (!fileExists(workPool, id, zipEntry.getName(), index)) {
												workPool.storeLargeAssociated(new StoreLargeAssociated(id, index, zipEntry.getName(),
														new ByteArrayInputStream(inputStream.readAllBytes()), true));
											}
										}
										else {
											workPool.storeLargeAssociated(new StoreLargeAssociated(id, index, zipEntry.getName(),
													new ByteArrayInputStream(inputStream.readAllBytes()), true));
										}
									}
									catch (Throwable t) {
										LOG.log(Level.SEVERE, "Could not restore associated file <" + zipEntry.getName() + ">", t);
									}
								}
							}

						}

						int i = count.incrementAndGet();
						if (i % 10000 == 0) {
							LOG.info("So far indexed <" + i + "> for index <" + index + ">");
						}
						return null;
					}
					catch (Exception e) {
						LOG.log(Level.SEVERE, e.getMessage());
						return null;
					}
				});
			}
		}
		finally {
			threadPool.shutdown();
		}

	}

	private static boolean fileExists(ZuliaWorkPool zuliaWorkPool, String id, String fileName, String indexName) throws Exception {

		Fetch fetchAssociated = new FetchAllAssociated(id, indexName).setAssociatedFetchType(META);

		FetchResult fetch = zuliaWorkPool.fetch(fetchAssociated);
		if (fetch.getAssociatedDocumentCount() > 0) {
			for (AssociatedResult assDoc : fetch.getAssociatedDocuments()) {
				if (assDoc.getFilename().equals(fileName)) {
					return true;
				}
			}
		}

		return false;

	}
}
