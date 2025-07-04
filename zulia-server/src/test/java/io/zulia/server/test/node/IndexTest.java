package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.UpdateIndex;
import io.zulia.client.command.builder.FieldMapping;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.ScoredQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.GetIndexConfigResult;
import io.zulia.client.result.UpdateIndexResult;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaIndex.AnalyzerSettings.Filter;
import io.zulia.message.ZuliaIndex.IndexSettings;
import io.zulia.message.ZuliaServiceOuterClass;
import io.zulia.server.test.node.shared.NodeExtension;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class IndexTest {
	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(3);
	public static final String INDEX_TEST = "indexTest";

	@Test
	@Order(1)
	public void createIndex() throws Exception {

		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		{
			ClientIndexConfig indexConfig = new ClientIndexConfig();
			indexConfig.addDefaultSearchField("title");
			indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
			indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD, "myTitle").sortAs("mySortTitle"));
			indexConfig.addFieldConfig(
					FieldConfigBuilder.createString("category").indexAs(DefaultAnalyzers.STANDARD).indexAs(DefaultAnalyzers.KEYWORD, "categoryExact")
							.sortAs(ZuliaIndex.SortAs.StringHandling.FOLDING, "categoryFolded").sort());
			indexConfig.addFieldConfig(
					FieldConfigBuilder.createDouble("rating").index().sort().description("Some optional description").displayName("Product Rating"));

			indexConfig.addWarmingSearch(new Search(INDEX_TEST).setSearchLabel("searching for stuff").addQuery(new ScoredQuery("title:stuff")));
			indexConfig.addWarmingSearch(
					new Search(INDEX_TEST).setSearchLabel("searching for other stuff").addQuery(new ScoredQuery("title:other AND title:stuff"))
							.setPinToCache(true));

			indexConfig.setIndexName(INDEX_TEST);
			indexConfig.setDisableCompression(true);
			indexConfig.setNumberOfShards(1);
			indexConfig.setDefaultConcurrency(12);

			zuliaWorkPool.createIndex(indexConfig);
		}

		{
			Assertions.assertThrows(Exception.class, () -> {
				@SuppressWarnings("unused") GetIndexConfigResult indexConfigFromServer = zuliaWorkPool.getIndexConfig("made up");
			}, "Index <made up> does not exist");
		}

		{

			ClientIndexConfig indexConfigFromServer = zuliaWorkPool.getIndexConfig(INDEX_TEST).getIndexConfig();

			Assertions.assertTrue(indexConfigFromServer.getDisableCompression());
			Assertions.assertEquals(12, indexConfigFromServer.getDefaultConcurrency());
			Assertions.assertEquals(4, indexConfigFromServer.getFieldConfigMap().size());

			ZuliaIndex.FieldConfig idFieldConfig = indexConfigFromServer.getFieldConfig("id");
			Assertions.assertTrue(idFieldConfig.getFacetAsList().isEmpty());
			Assertions.assertEquals(1, idFieldConfig.getSortAsCount());
			Assertions.assertEquals("id", idFieldConfig.getSortAsList().getFirst().getSortFieldName());
			Assertions.assertEquals(1, idFieldConfig.getIndexAsCount());
			Assertions.assertEquals("id", idFieldConfig.getIndexAsList().getFirst().getIndexFieldName());
			Assertions.assertEquals(DefaultAnalyzers.LC_KEYWORD, idFieldConfig.getIndexAsList().getFirst().getAnalyzerName());

			ZuliaIndex.FieldConfig titleFieldConfig = indexConfigFromServer.getFieldConfig("title");
			Assertions.assertTrue(titleFieldConfig.getFacetAsList().isEmpty());
			Assertions.assertEquals(1, titleFieldConfig.getSortAsCount());
			Assertions.assertEquals("mySortTitle", titleFieldConfig.getSortAsList().getFirst().getSortFieldName());
			Assertions.assertEquals(1, titleFieldConfig.getIndexAsCount());
			Assertions.assertEquals("myTitle", titleFieldConfig.getIndexAsList().getFirst().getIndexFieldName());
			Assertions.assertEquals(DefaultAnalyzers.STANDARD, titleFieldConfig.getIndexAsList().getFirst().getAnalyzerName());

			ZuliaIndex.FieldConfig categoryFieldConfig = indexConfigFromServer.getFieldConfig("category");
			Assertions.assertTrue(categoryFieldConfig.getFacetAsList().isEmpty());
			Assertions.assertEquals(2, categoryFieldConfig.getSortAsCount());
			Assertions.assertEquals("categoryFolded", categoryFieldConfig.getSortAsList().get(0).getSortFieldName());
			Assertions.assertEquals("category", categoryFieldConfig.getSortAsList().get(1).getSortFieldName());
			Assertions.assertEquals(2, categoryFieldConfig.getIndexAsCount());
			Assertions.assertEquals("category", categoryFieldConfig.getIndexAsList().get(0).getIndexFieldName());
			Assertions.assertEquals(DefaultAnalyzers.STANDARD, categoryFieldConfig.getIndexAsList().get(0).getAnalyzerName());
			Assertions.assertEquals("categoryExact", categoryFieldConfig.getIndexAsList().get(1).getIndexFieldName());
			Assertions.assertEquals(DefaultAnalyzers.KEYWORD, categoryFieldConfig.getIndexAsList().get(1).getAnalyzerName());

			ZuliaIndex.FieldConfig ratingField = indexConfigFromServer.getFieldConfig("rating");
			Assertions.assertTrue(ratingField.getFacetAsList().isEmpty());
			Assertions.assertEquals(1, ratingField.getSortAsCount());
			Assertions.assertEquals("rating", ratingField.getSortAsList().getFirst().getSortFieldName());
			Assertions.assertEquals(1, ratingField.getIndexAsCount());
			Assertions.assertEquals("rating", ratingField.getIndexAsList().getFirst().getIndexFieldName());
			Assertions.assertEquals("", ratingField.getIndexAsList().getFirst().getAnalyzerName());
			Assertions.assertEquals("Some optional description", ratingField.getDescription());
			Assertions.assertEquals("Product Rating", ratingField.getDisplayName());

			List<String> defaultSearchFields = indexConfigFromServer.getDefaultSearchFields();
			Assertions.assertEquals(1, defaultSearchFields.size());
			Assertions.assertEquals("title", defaultSearchFields.getFirst());

			List<ZuliaServiceOuterClass.QueryRequest> warmingSearches = indexConfigFromServer.getWarmingSearches();
			Assertions.assertEquals(2, warmingSearches.size());
			Assertions.assertEquals("searching for stuff", warmingSearches.getFirst().getSearchLabel());
			Assertions.assertFalse(warmingSearches.getFirst().getPinToCache());
			Assertions.assertEquals(1, warmingSearches.get(0).getQueryList().size());
			Assertions.assertEquals("title:stuff", warmingSearches.get(0).getQuery(0).getQ());
			Assertions.assertEquals("searching for other stuff", warmingSearches.get(1).getSearchLabel());
			Assertions.assertTrue(warmingSearches.get(1).getPinToCache());
			Assertions.assertEquals(1, warmingSearches.get(1).getQueryList().size());
			Assertions.assertEquals("title:other AND title:stuff", warmingSearches.get(1).getQuery(0).getQ());
		}

		{
			ClientIndexConfig indexConfig = new ClientIndexConfig();
			indexConfig.addDefaultSearchFields("title", "category");
			indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
			indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD, "myTitle").sortAs("mySortTitle"));
			indexConfig.addFieldConfig(
					FieldConfigBuilder.createDouble("rating").index().sort().description("Some optional description").displayName("Product Rating"));
			indexConfig.setIndexName(INDEX_TEST);
			indexConfig.setNumberOfShards(1);
			indexConfig.addWarmingSearch(new Search(INDEX_TEST).setSearchLabel("searching the stars").addQuery(new ScoredQuery("title:stars")));
			indexConfig.addWarmingSearch(
					new Search(INDEX_TEST).setSearchLabel("searching for cash").addQuery(new ScoredQuery("title:cash")).setPinToCache(true));
			indexConfig.addFieldMapping(new FieldMapping("title").addMappedFields("category").includeSelf());
			indexConfig.addFieldMapping(new FieldMapping("test").addMappedFields("title", "category"));
			indexConfig.setDisableCompression(false);

			zuliaWorkPool.createIndex(indexConfig);

		}

		{
			ClientIndexConfig indexConfigFromServer = zuliaWorkPool.getIndexConfig(INDEX_TEST).getIndexConfig();

			Assertions.assertFalse(indexConfigFromServer.getDisableCompression());

			Assertions.assertEquals(3, indexConfigFromServer.getFieldConfigMap().size());

			List<String> defaultSearchFields = indexConfigFromServer.getDefaultSearchFields();
			Assertions.assertEquals(2, defaultSearchFields.size());
			Assertions.assertEquals("title", defaultSearchFields.get(0));
			Assertions.assertEquals("category", defaultSearchFields.get(1));
		}

	}

	@Test
	@Order(2)
	public void updateIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		{
			UpdateIndex updateIndex = new UpdateIndex(INDEX_TEST);
			updateIndex.setIndexWeight(4);
			updateIndex.setDisableCompression(true);
			updateIndex.setDefaultConcurrency(5);

			FieldConfigBuilder newField = FieldConfigBuilder.createString("newField").indexAs(DefaultAnalyzers.LC_KEYWORD).sort();
			updateIndex.mergeFieldConfig(newField);

			zuliaWorkPool.updateIndex(updateIndex);

		}

		{

			ClientIndexConfig indexConfigFromServer = zuliaWorkPool.getIndexConfig(INDEX_TEST).getIndexConfig();

			Assertions.assertEquals(4, indexConfigFromServer.getIndexWeight());
			Assertions.assertTrue(indexConfigFromServer.getDisableCompression());
			Assertions.assertEquals(5, indexConfigFromServer.getDefaultConcurrency());
			Assertions.assertEquals(4, indexConfigFromServer.getFieldConfigMap().size());
			ZuliaIndex.FieldConfig newField = indexConfigFromServer.getFieldConfig("newField");
			Assertions.assertEquals(1, newField.getSortAsCount());
			Assertions.assertEquals("newField", newField.getSortAsList().getFirst().getSortFieldName());
			Assertions.assertEquals(1, newField.getIndexAsCount());
			Assertions.assertEquals("newField", newField.getIndexAsList().getFirst().getIndexFieldName());
			Assertions.assertEquals(DefaultAnalyzers.LC_KEYWORD, newField.getIndexAsList().getFirst().getAnalyzerName());
			Assertions.assertEquals(new Document(), indexConfigFromServer.getMeta());
		}

		{
			UpdateIndex updateIndex = new UpdateIndex(INDEX_TEST);
			updateIndex.mergeMetadata(new Document().append("someKey", 5).append("otherKey", "a string"));

			FieldConfigBuilder newField2 = FieldConfigBuilder.createString("newField2").indexAs(DefaultAnalyzers.STANDARD).sort();
			updateIndex.replaceFieldConfig(newField2);

			zuliaWorkPool.updateIndex(updateIndex);

		}

		{

			ClientIndexConfig indexConfigFromServer = zuliaWorkPool.getIndexConfig(INDEX_TEST).getIndexConfig();

			Assertions.assertEquals(1, indexConfigFromServer.getFieldConfigMap().size());
			ZuliaIndex.FieldConfig newField = indexConfigFromServer.getFieldConfig("newField2");
			Assertions.assertEquals(1, newField.getSortAsCount());
			Assertions.assertEquals("newField2", newField.getSortAsList().getFirst().getSortFieldName());
			Assertions.assertEquals(1, newField.getIndexAsCount());
			Assertions.assertEquals("newField2", newField.getIndexAsList().getFirst().getIndexFieldName());
			Assertions.assertEquals(DefaultAnalyzers.STANDARD, newField.getIndexAsList().getFirst().getAnalyzerName());
			Document meta = indexConfigFromServer.getMeta();
			Assertions.assertEquals(5, meta.getInteger("someKey"));
			Assertions.assertEquals("a string", meta.getString("otherKey"));
		}

		{
			UpdateIndex updateIndex = new UpdateIndex(INDEX_TEST);
			updateIndex.mergeMetadata(new Document().append("someKey", 10));
			updateIndex.removeMetadataByKey(List.of("otherKey"));

			FieldConfigBuilder myField = FieldConfigBuilder.createString("myField").indexAs(DefaultAnalyzers.STANDARD).sort();
			FieldConfigBuilder otherField = FieldConfigBuilder.createString("otherField").indexAs(DefaultAnalyzers.LC_KEYWORD).sort();
			updateIndex.mergeFieldConfig(myField, otherField);

			zuliaWorkPool.updateIndex(updateIndex);

		}

		{

			ClientIndexConfig indexConfigFromServer = zuliaWorkPool.getIndexConfig(INDEX_TEST).getIndexConfig();

			Assertions.assertEquals(3, indexConfigFromServer.getFieldConfigMap().size());

			Document meta = indexConfigFromServer.getMeta();
			Assertions.assertEquals(10, meta.getInteger("someKey"));
			Assertions.assertNull(meta.get("otherKey"));
		}

		{
			UpdateIndex updateIndex = new UpdateIndex(INDEX_TEST);
			updateIndex.replaceMetadata(new Document().append("stuff", "for free"));

			FieldConfigBuilder myField = FieldConfigBuilder.createString("myField").indexAs(DefaultAnalyzers.STANDARD).sort();
			FieldConfigBuilder otherField = FieldConfigBuilder.createString("otherField").indexAs(DefaultAnalyzers.LC_KEYWORD).sort();
			updateIndex.mergeFieldConfig(myField, otherField);

			zuliaWorkPool.updateIndex(updateIndex);

		}

		{
			ClientIndexConfig indexConfigFromServer = zuliaWorkPool.getIndexConfig(INDEX_TEST).getIndexConfig();

			Assertions.assertEquals(3, indexConfigFromServer.getFieldConfigMap().size());

			Document meta = indexConfigFromServer.getMeta();
			Assertions.assertEquals("for free", meta.getString("stuff"));
			Assertions.assertNull(meta.get("otherKey"));
			Assertions.assertNull(meta.get("someKey"));

			Assertions.assertEquals(0, indexConfigFromServer.getAnalyzerSettingsMap().size());
		}

		{

			UpdateIndex updateIndex = new UpdateIndex(INDEX_TEST);
			FieldConfigBuilder myField = FieldConfigBuilder.createString("myField").indexAs("custom").sort();
			updateIndex.mergeFieldConfig(myField);

			Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.updateIndex(updateIndex),
					"Analyzer <custom> is not a default analyzer and is not given as a custom analyzer for field <myField> indexed as <myField>");

			ZuliaIndex.AnalyzerSettings custom = ZuliaIndex.AnalyzerSettings.newBuilder().setName("custom").addFilter(Filter.LOWERCASE).build();
			ZuliaIndex.AnalyzerSettings mine = ZuliaIndex.AnalyzerSettings.newBuilder().setName("mine").addFilter(Filter.LOWERCASE).addFilter(Filter.BRITISH_US)
					.build();
			updateIndex.mergeAnalyzerSettings(custom, mine);
			zuliaWorkPool.updateIndex(updateIndex);
		}

		{
			ClientIndexConfig indexConfigFromServer = zuliaWorkPool.getIndexConfig(INDEX_TEST).getIndexConfig();

			Assertions.assertEquals(2, indexConfigFromServer.getAnalyzerSettingsMap().size());

			ZuliaIndex.AnalyzerSettings custom = indexConfigFromServer.getAnalyzerSettings("custom");
			Assertions.assertEquals("custom", custom.getName());
			Assertions.assertEquals(1, custom.getFilterCount());
			Assertions.assertEquals(Filter.LOWERCASE, custom.getFilterList().getFirst());

			ZuliaIndex.AnalyzerSettings mine = indexConfigFromServer.getAnalyzerSettings("mine");
			Assertions.assertEquals("mine", mine.getName());
			Assertions.assertEquals(2, mine.getFilterCount());
			Assertions.assertEquals(Filter.LOWERCASE, mine.getFilterList().get(0));
			Assertions.assertEquals(Filter.BRITISH_US, mine.getFilterList().get(1));
		}

		{

			UpdateIndex updateIndex = new UpdateIndex(INDEX_TEST);
			ZuliaIndex.AnalyzerSettings custom = ZuliaIndex.AnalyzerSettings.newBuilder().setName("custom").addFilter(Filter.LOWERCASE)
					.addFilter(Filter.ASCII_FOLDING).build();
			updateIndex.mergeAnalyzerSettings(custom);
			zuliaWorkPool.updateIndex(updateIndex);
		}

		{
			ClientIndexConfig indexConfigFromServer = zuliaWorkPool.getIndexConfig(INDEX_TEST).getIndexConfig();

			Assertions.assertEquals(2, indexConfigFromServer.getAnalyzerSettingsMap().size());

			ZuliaIndex.AnalyzerSettings custom = indexConfigFromServer.getAnalyzerSettings("custom");
			Assertions.assertEquals("custom", custom.getName());
			Assertions.assertEquals(2, custom.getFilterCount());
			Assertions.assertEquals(Filter.LOWERCASE, custom.getFilterList().get(0));
			Assertions.assertEquals(Filter.ASCII_FOLDING, custom.getFilterList().get(1));

			ZuliaIndex.AnalyzerSettings mine = indexConfigFromServer.getAnalyzerSettings("mine");
			Assertions.assertEquals("mine", mine.getName());
			Assertions.assertEquals(2, mine.getFilterCount());
			Assertions.assertEquals(Filter.LOWERCASE, mine.getFilterList().get(0));
			Assertions.assertEquals(Filter.BRITISH_US, mine.getFilterList().get(1));
		}

		{

			UpdateIndex updateIndex = new UpdateIndex(INDEX_TEST);
			updateIndex.removeAnalyzerSettingsByName("mine");
			zuliaWorkPool.updateIndex(updateIndex);
		}

		{

			UpdateIndex updateIndex = new UpdateIndex(INDEX_TEST);
			updateIndex.removeAnalyzerSettingsByName("custom");

			Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.updateIndex(updateIndex),
					"Analyzer <custom> is not a default analyzer and is not given as a custom analyzer for field <myField> indexed as <myField>");
		}

		{
			UpdateIndex updateIndex = new UpdateIndex(INDEX_TEST);
			updateIndex.removeAnalyzerSettingsByName("custom");
			FieldConfigBuilder myField = FieldConfigBuilder.createString("myField").indexAs(DefaultAnalyzers.STANDARD).sort();
			updateIndex.mergeFieldConfig(myField);
			UpdateIndexResult updateIndexResult = zuliaWorkPool.updateIndex(updateIndex);

			IndexSettings indexSettings = updateIndexResult.getFullIndexSettings();
			Assertions.assertEquals(3, indexSettings.getFieldConfigList().size());
			Assertions.assertEquals(0, indexSettings.getAnalyzerSettingsCount());
			Assertions.assertEquals(4, indexSettings.getIndexWeight());
		}

		{
			UpdateIndex updateIndex = new UpdateIndex(INDEX_TEST);
			updateIndex.mergeWarmingSearches(new Search(INDEX_TEST).addQuery(new FilterQuery("different query")).setSearchLabel("searching the stars"));

			UpdateIndexResult updateIndexResult = zuliaWorkPool.updateIndex(updateIndex);

			ClientIndexConfig clientIndexConfig = updateIndexResult.getClientIndexConfig();
			Assertions.assertEquals(2, clientIndexConfig.getWarmingSearches().size());
			Assertions.assertEquals("different query", clientIndexConfig.getWarmingSearches().getFirst().getQuery(0).getQ());

		}

		{
			UpdateIndex updateIndex = new UpdateIndex(INDEX_TEST);
			updateIndex.removeWarmingSearchesByLabel("not exist");

			UpdateIndexResult updateIndexResult = zuliaWorkPool.updateIndex(updateIndex);

			IndexSettings indexSettings = updateIndexResult.getFullIndexSettings();
			Assertions.assertEquals(2, indexSettings.getWarmingSearchesCount());

		}

		{
			UpdateIndex updateIndex = new UpdateIndex(INDEX_TEST);
			updateIndex.removeWarmingSearchesByLabel("searching the stars");

			UpdateIndexResult updateIndexResult = zuliaWorkPool.updateIndex(updateIndex);

			IndexSettings indexSettings = updateIndexResult.getFullIndexSettings();
			Assertions.assertEquals(1, indexSettings.getWarmingSearchesCount());

		}

		{
			UpdateIndex updateIndex = new UpdateIndex(INDEX_TEST);
			updateIndex.replaceWarmingSearches(new Search(INDEX_TEST).addQuery(new FilterQuery("some stuff")).setSearchLabel("the best label"),
					new Search(INDEX_TEST).addQuery(new FilterQuery("more stuff")).setSearchLabel("the better label"));

			UpdateIndexResult updateIndexResult = zuliaWorkPool.updateIndex(updateIndex);

			ClientIndexConfig clientIndexConfig = updateIndexResult.getClientIndexConfig();
			Assertions.assertEquals(2, clientIndexConfig.getWarmingSearches().size());
			Assertions.assertEquals("some stuff", clientIndexConfig.getWarmingSearches().get(0).getQuery(0).getQ());
			Assertions.assertEquals("more stuff", clientIndexConfig.getWarmingSearches().get(1).getQuery(0).getQ());

		}

		{
			UpdateIndex updateIndex = new UpdateIndex(INDEX_TEST);
			updateIndex.removeFieldMappingByAlias("not exist");
			UpdateIndexResult updateIndexResult = zuliaWorkPool.updateIndex(updateIndex);

			IndexSettings indexSettings = updateIndexResult.getFullIndexSettings();
			Assertions.assertEquals(2, indexSettings.getFieldMappingList().size());
			//check some other fields to make sure they didn't change, only field mapping
			Assertions.assertEquals(2, indexSettings.getWarmingSearchesCount());
			Assertions.assertEquals(4, indexSettings.getIndexWeight());
		}

		{
			UpdateIndex updateIndex = new UpdateIndex(INDEX_TEST);
			updateIndex.removeFieldMappingByAlias("test");
			UpdateIndexResult updateIndexResult = zuliaWorkPool.updateIndex(updateIndex);

			IndexSettings indexSettings = updateIndexResult.getFullIndexSettings();
			Assertions.assertEquals(1, indexSettings.getFieldMappingList().size());
			Assertions.assertEquals("title", indexSettings.getFieldMappingList().getFirst().getAlias());
			//check some other fields to make sure they didn't change, only field mapping
			Assertions.assertEquals(2, indexSettings.getWarmingSearchesCount());
			Assertions.assertEquals(4, indexSettings.getIndexWeight());
		}

		{
			UpdateIndex updateIndex = new UpdateIndex(INDEX_TEST);

			FieldMapping fieldMapping = new FieldMapping("test1").addMappedFields("someField", "someField2");
			FieldMapping fieldMapping2 = new FieldMapping("test2").addMappedFields("someField2", "someField3");
			updateIndex.replaceFieldMapping(fieldMapping, fieldMapping2);
			UpdateIndexResult updateIndexResult = zuliaWorkPool.updateIndex(updateIndex);

			IndexSettings indexSettings = updateIndexResult.getFullIndexSettings();
			Assertions.assertEquals(2, indexSettings.getFieldMappingList().size());
			Assertions.assertEquals("test1", indexSettings.getFieldMappingList().get(0).getAlias());
			Assertions.assertEquals("test2", indexSettings.getFieldMappingList().get(1).getAlias());
			//check some other fields to make sure they didn't change, only field mapping
			Assertions.assertEquals(2, indexSettings.getWarmingSearchesCount());
			Assertions.assertEquals(4, indexSettings.getIndexWeight());
		}

		{
			UpdateIndex updateIndex = new UpdateIndex(INDEX_TEST);

			FieldMapping fieldMapping2 = new FieldMapping("test2").addMappedFields("someField2", "someField3", "someField4");
			FieldMapping fieldMapping = new FieldMapping("test3").addMappedFields("someField100", "someField101");
			updateIndex.mergeFieldMapping(fieldMapping, fieldMapping2);
			UpdateIndexResult updateIndexResult = zuliaWorkPool.updateIndex(updateIndex);

			IndexSettings indexSettings = updateIndexResult.getFullIndexSettings();
			Assertions.assertEquals(3, indexSettings.getFieldMappingList().size());
			Assertions.assertEquals("test1", indexSettings.getFieldMappingList().get(0).getAlias());
			Assertions.assertEquals("test2", indexSettings.getFieldMappingList().get(1).getAlias());
			Assertions.assertEquals("test3", indexSettings.getFieldMappingList().get(2).getAlias());
			//check some other fields to make sure they didn't change, only field mapping
			Assertions.assertEquals(2, indexSettings.getWarmingSearchesCount());
			Assertions.assertEquals(4, indexSettings.getIndexWeight());
		}

		{
			UpdateIndex updateIndex = new UpdateIndex(INDEX_TEST);
			updateIndex.removeAllWarmingSearches();

			UpdateIndexResult updateIndexResult = zuliaWorkPool.updateIndex(updateIndex);

			ClientIndexConfig clientIndexConfig = updateIndexResult.getClientIndexConfig();
			Assertions.assertEquals(0, clientIndexConfig.getWarmingSearches().size());

			IndexSettings indexSettings = updateIndexResult.getFullIndexSettings();
			Assertions.assertEquals(3, indexSettings.getFieldMappingList().size());
		}

		{
			UpdateIndex updateIndex = new UpdateIndex(INDEX_TEST);
			updateIndex.removeAllFieldMappings();

			UpdateIndexResult updateIndexResult = zuliaWorkPool.updateIndex(updateIndex);

			IndexSettings indexSettings = updateIndexResult.getFullIndexSettings();
			Assertions.assertEquals(0, indexSettings.getFieldMappingList().size());

		}

		{
			UpdateIndex updateIndex = new UpdateIndex(INDEX_TEST);
			updateIndex.replaceMetadata(new Document().append("stuff", "for free").append("hello", "world").append("the", "best"));
			UpdateIndexResult updateIndexResult = zuliaWorkPool.updateIndex(updateIndex);

			Assertions.assertEquals(3, updateIndexResult.getClientIndexConfig().getMeta().size());
		}

		{
			UpdateIndex updateIndex = new UpdateIndex(INDEX_TEST);
			updateIndex.removeAllMetadata();
			UpdateIndexResult updateIndexResult = zuliaWorkPool.updateIndex(updateIndex);
			Assertions.assertEquals(0, updateIndexResult.getClientIndexConfig().getMeta().size());
		}
	}

	@Test
	@Order(3)
	public void giantIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		{
			ClientIndexConfig indexConfig = new ClientIndexConfig();
			indexConfig.addDefaultSearchField("title");
			indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
			indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD, "myTitle").sortAs("mySortTitle"));

			for (int i = 0; i < 100000; i++) {
				indexConfig.addFieldConfig(FieldConfigBuilder.createDouble("rating" + i).index().sort().description("Some optional description")
						.displayName("Product Rating " + i));
			}

			indexConfig.setIndexName("large test");
			indexConfig.setNumberOfShards(1);

			zuliaWorkPool.createIndex(indexConfig);
		}

		{
			GetIndexConfigResult largeTest = zuliaWorkPool.getIndexConfig("large test");
			Assertions.assertEquals(100002, largeTest.getIndexConfig().getFieldConfigMap().size());
		}

	}

}
