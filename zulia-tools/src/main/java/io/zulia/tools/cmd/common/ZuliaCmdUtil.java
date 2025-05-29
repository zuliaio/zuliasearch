package io.zulia.tools.cmd.common;

import com.google.common.base.Charsets;
import io.zulia.ZuliaConstants;
import io.zulia.client.command.Fetch;
import io.zulia.client.command.FetchAllAssociated;
import io.zulia.client.command.Store;
import io.zulia.client.command.StoreLargeAssociated;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.Sort;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.AssociatedResult;
import io.zulia.client.result.FetchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.util.pool.TaskExecutor;
import io.zulia.util.pool.WorkPool;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;

import static io.zulia.message.ZuliaQuery.FetchType.META;

public class ZuliaCmdUtil {
	public enum AssociatedFilesHandling {
		skip,
		skipExisting,
		overwrite
	}

	private static final Logger LOG = LoggerFactory.getLogger(ZuliaCmdUtil.class);

	public static void writeOutput(String recordsFilename, String index, String q, int rows, ZuliaWorkPool workPool, AtomicInteger count, Set<String> uniqueIds)
			throws Exception {
		try (FileWriter fileWriter = new FileWriter(recordsFilename, Charsets.UTF_8)) {

			Search zuliaQuery = new Search(index).addQuery(new FilterQuery(q)).setAmount(rows);
			zuliaQuery.addSort(new Sort(ZuliaConstants.ID_SORT_FIELD));

			try {
				workPool.searchAll(zuliaQuery, queryResult -> {

					long totalHits = queryResult.getTotalHits();

					queryResult.getCompleteResults().forEach(completeResult -> {
						try {
							if (uniqueIds != null) {
								uniqueIds.add(completeResult.getUniqueId());
							}
							fileWriter.write(completeResult.getDocument().toJson());
							fileWriter.write(System.lineSeparator());

							int c = count.incrementAndGet();
							if (c % 1000 == 0) {
								LOG.info("So far written {} of {} for index {}", c, totalHits, index);
							}

						}
						catch (IOException e) {
							LOG.error("Could not write record {} for index {}", completeResult.getUniqueId(), index, e);
						}
						catch (Throwable e) {
							LOG.error("Could not write output for index {}", index, e);
						}

					});
				});
			}
			catch (Throwable t) {
				LOG.error("Query failed for index {}", index, t);
			}

		}
		catch (Throwable e) {
			LOG.error("Could not write output for index {}", index, e);
			throw e;
		}
	}

	public static void index(String inputDir, String recordsFilename, String idField, String index, ZuliaWorkPool workPool, AtomicInteger count,
			Integer threads, AssociatedFilesHandling associatedFilesHandling) throws Exception {
		try (TaskExecutor threadPool = WorkPool.nativePool(threads)) {
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

							String fullPathToFile = inputDir + File.separator + id.replaceAll("/", "_") + ".zip";
							if (Files.exists(Paths.get(fullPathToFile))) {

								File destDir = new File(inputDir + File.separator + UUID.randomUUID() + "_tempWork");
								byte[] buffer = new byte[1024];
								try (ZipArchiveInputStream inputStream = new ZipArchiveInputStream(new FileInputStream(Paths.get(fullPathToFile).toFile()))) {
									ZipArchiveEntry zipEntry;
									while ((zipEntry = inputStream.getNextZipEntry()) != null) {
										decompressZipEntryToDisk(destDir, buffer, inputStream, zipEntry);
									}
								}

								// ensure the file was extractable
								if (!AssociatedFilesHandling.skip.equals(associatedFilesHandling) && Files.exists(destDir.toPath())) {

									List<Path> tempFiles;
									try (Stream<Path> sp = Files.list(destDir.toPath())) {
										tempFiles = sp.toList();
									}
									for (Path path : tempFiles) {
										if (path.toFile().isDirectory()) {
											try {

												List<Path> filesPaths;
												try (Stream<Path> sp = Files.list(path)) {
													filesPaths = sp.toList();
												}

												Document meta = null;

												String filename = null;
												File file = null;
												for (Path filePath : filesPaths) {
													try {
														if (filePath.toFile().getName().endsWith("_metadata.json")) {
															meta = Document.parse(Files.readString(filePath));
														}
														else {
															file = filePath.toFile();
															filename = filePath.toFile().getName();
														}
													}
													catch (Throwable t) {
														LOG.error("Could not restore associated file {}", filename, t);
													}
												}

												if (AssociatedFilesHandling.skipExisting.equals(associatedFilesHandling)) {
													if (!fileExists(workPool, id, filename, index)) {
														storeAssociatedDoc(index, workPool, id, filename, meta, file);
													}
												}
												else {
													storeAssociatedDoc(index, workPool, id, filename, meta, file);
												}
											}
											catch (Throwable t) {
												LOG.error("Could not list the individual files for dir {}", path.getFileName(), t);
											}
										}
										else {
											LOG.error("Top level file that shouldn't exist: {}", path.getFileName());
										}
									}

									// clean up temp work
									try (Stream<Path> walk = Files.walk(destDir.toPath())) {
										walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
										destDir.delete();
									}

								}
								else {
									//LOG.error("Could not extract file {}", fullPathToFile);
								}

							}

							int i = count.incrementAndGet();
							if (i % 10000 == 0) {
								LOG.info("So far indexed {} for index {}", i, index);
							}
							return null;
						}
						catch (Exception e) {
							LOG.error(e.getMessage(), e);
							return null;
						}
					});
				}
			}
		}

	}

	/**
	 * Based on: https://www.baeldung.com/java-compress-and-uncompress
	 *
	 * @param destDir
	 * @param buffer
	 * @param inputStream
	 * @param zipEntry
	 * @throws IOException
	 */
	private static void decompressZipEntryToDisk(File destDir, byte[] buffer, ZipArchiveInputStream inputStream, ZipArchiveEntry zipEntry) throws IOException {
		File newFile = newFile(destDir, zipEntry);
		if (zipEntry.isDirectory()) {
			if (!newFile.isDirectory() && !newFile.mkdirs()) {
				throw new IOException("Failed to create directory " + newFile);
			}
		}
		else {
			// fix for Windows-created archives
			File parent = newFile.getParentFile();
			if (!parent.isDirectory() && !parent.mkdirs()) {
				throw new IOException("Failed to create directory " + parent);
			}

			// write file content
			FileOutputStream fos = new FileOutputStream(newFile);
			int len;
			while ((len = inputStream.read(buffer)) > 0) {
				fos.write(buffer, 0, len);
			}
			fos.close();
		}
	}

	private static void storeAssociatedDoc(String index, ZuliaWorkPool workPool, String id, String filename, Document meta, File file) throws Exception {
		workPool.storeLargeAssociated(new StoreLargeAssociated(id, index, filename, file).setMeta(meta));
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

	private static File newFile(File destinationDir, ZipEntry zipEntry) throws IOException {
		File destFile = new File(destinationDir, zipEntry.getName());

		String destDirPath = destinationDir.getCanonicalPath();
		String destFilePath = destFile.getCanonicalPath();

		if (!destFilePath.startsWith(destDirPath + File.separator)) {
			throw new IOException("Entry is outside of the target dir: " + zipEntry.getName());
		}

		return destFile;
	}

}
