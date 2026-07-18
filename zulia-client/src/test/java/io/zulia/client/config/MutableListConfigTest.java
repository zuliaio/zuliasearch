package io.zulia.client.config;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.UpdateIndex;
import io.zulia.client.command.builder.Search;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaIndex.AnalyzerSettings;
import io.zulia.message.ZuliaIndex.IndexSettings;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * The client command objects store lists that can come from protobuf (unmodifiable), List.of,
 * Arrays.asList, or a stream toList. Mutating methods must never call add or clear on them directly.
 */
public class MutableListConfigTest {

	@Test
	public void addDefaultSearchFieldAfterConfigure() {
		// configure() loads from IndexSettings, whose getDefaultSearchFieldList is unmodifiable
		IndexSettings settings = IndexSettings.newBuilder().setIndexName("existing").addDefaultSearchField("title").build();
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.configure(settings);

		// the read-modify-update flow: add a field to a loaded non-empty config
		indexConfig.addDefaultSearchField("abstract");
		Assertions.assertEquals(List.of("title", "abstract"), indexConfig.getDefaultSearchFields());

		indexConfig.addDefaultSearchFields("author", "keywords");
		Assertions.assertEquals(List.of("title", "abstract", "author", "keywords"), indexConfig.getDefaultSearchFields());
	}

	@Test
	public void addDefaultSearchFieldAfterImmutableSetter() {
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.setDefaultSearchFields(List.of("title"));
		indexConfig.addDefaultSearchField("abstract");
		Assertions.assertEquals(List.of("title", "abstract"), indexConfig.getDefaultSearchFields());
	}

	@Test
	public void addWarmingSearchAfterImmutableSetter() {
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.setWarmingSearches(List.of(new Search("idx").getRequest()));
		indexConfig.addWarmingSearch(new Search("idx"));
		Assertions.assertEquals(2, indexConfig.getWarmingSearches().size());
	}

	@Test
	public void updateIndexClearAfterMergeAndReplace() {
		AnalyzerSettings analyzer = AnalyzerSettings.newBuilder().setName(DefaultAnalyzers.STANDARD).build();

		// merge varargs stores a List.of, replace varargs stores an Arrays.asList,
		// mergeFieldConfig stores a stream toList: clearing pending changes must not throw on any
		UpdateIndex updateIndex = new UpdateIndex("idx");
		updateIndex.mergeAnalyzerSettings(analyzer);
		updateIndex.clearAnalyzerSettingsChanges();

		updateIndex.replaceAnalyzerSettings(analyzer);
		updateIndex.clearAnalyzerSettingsChanges();

		updateIndex.mergeFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD));
		updateIndex.clearFieldConfigChanges();

		updateIndex.mergeWarmingSearches(new Search("idx").setSearchLabel("warm"));
		updateIndex.clearWarmingSearchChanges();

		// cleared changes leave nothing pending in the request
		var request = updateIndex.getRequest();
		Assertions.assertFalse(request.getUpdateIndexSettings().getAnalyzerSettingsOperation().getEnable());
		Assertions.assertFalse(request.getUpdateIndexSettings().getFieldConfigOperation().getEnable());
		Assertions.assertFalse(request.getUpdateIndexSettings().getWarmingSearchesOperation().getEnable());
	}
}
