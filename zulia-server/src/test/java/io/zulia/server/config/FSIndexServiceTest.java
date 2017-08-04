package io.zulia.server.config;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.zulia.message.ZuliaIndex;
import io.zulia.server.config.single.FSIndexService;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Created by Payam Meyer on 8/4/17.
 * @author pmeyer
 */
public class FSIndexServiceTest {

	private static final Gson GSON = new GsonBuilder().create();

	private IndexService indexService;
	private ZuliaIndex.IndexSettings indexOne;
	private ZuliaIndex.IndexSettings indexTwo;
	private ZuliaIndex.IndexSettings indexThree;
	private ZuliaIndex.IndexMapping indexMappingOne;
	private ZuliaIndex.IndexMapping indexMappingTwo;
	private ZuliaIndex.IndexMapping indexMappingThree;
	private ZuliaConfig zuliaConfig;

	@BeforeTest(enabled = false)
	public void init() throws FileNotFoundException {

		zuliaConfig = GSON
				.fromJson(new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream("zulia_test.json")), ZuliaConfig.class);

		indexService = new FSIndexService(zuliaConfig);

		indexOne = ZuliaIndex.IndexSettings.newBuilder().setIndexName("indexOne").build();
		indexTwo = ZuliaIndex.IndexSettings.newBuilder().setIndexName("indexTwo").build();
		indexThree = ZuliaIndex.IndexSettings.newBuilder().setIndexName("indexThree").build();

		indexMappingOne = ZuliaIndex.IndexMapping.newBuilder().setIndexName("indexOne").setNumberOfShards(5).build();
		indexMappingTwo = ZuliaIndex.IndexMapping.newBuilder().setIndexName("indexTwo").setNumberOfShards(2).build();
		indexMappingThree = ZuliaIndex.IndexMapping.newBuilder().setIndexName("indexThree").setNumberOfShards(4).build();

	}

	@AfterTest(enabled = false)
	public void cleanup() throws IOException {
		Files.deleteIfExists(Paths.get(zuliaConfig.getDataPath() + File.separator + "settings"));
	}

	@Test(enabled = false)
	public void testCreateIndex() throws Exception {
		indexService.createIndex(indexOne);
		indexService.createIndex(indexTwo);
		indexService.createIndex(indexThree);
	}

	@Test(enabled = false)
	public void testCreateIndexMapping() throws Exception {
		indexService.storeIndexMapping(indexMappingOne);
		indexService.storeIndexMapping(indexMappingTwo);
		indexService.storeIndexMapping(indexMappingThree);
	}

	@Test(enabled = false)
	public void testRemoveIndex() throws Exception {
		indexService.removeIndex(indexOne.getIndexName());
		indexService.removeIndex(indexTwo.getIndexName());
		indexService.removeIndex(indexThree.getIndexName());
	}

}
