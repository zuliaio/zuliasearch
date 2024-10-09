package io.zulia.tools.cmd.zuliaadmin;

import com.google.protobuf.util.JsonFormat;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.GetIndexConfigResult;
import io.zulia.cmd.common.ZuliaCommonCmd;
import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaServiceOuterClass;
import io.zulia.tools.cmd.ZuliaAdmin;
import io.zulia.tools.cmd.common.MultipleIndexArgs;
import org.bson.Document;
import picocli.CommandLine;

import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "displayIndex", description = "Display index(es) settings specified by --index")
public class DisplayIndexCmd implements Callable<Integer> {
	public enum IndexSettings {
		Meta,
		FieldMapping,
		FieldConfig,
		WarmedSearches
	}

	@CommandLine.ParentCommand
	private ZuliaAdmin zuliaAdmin;

	@CommandLine.Mixin
	private MultipleIndexArgs multipleIndexArgs;

	@CommandLine.Option(names = "--view", description = "Valid values: ${COMPLETION-CANDIDATES}")
	public IndexSettings view;

	@Override
	public Integer call() throws Exception {

		ZuliaWorkPool zuliaWorkPool = zuliaAdmin.getConnection();

		Set<String> indexes = multipleIndexArgs.resolveIndexes(zuliaWorkPool);

		for (String index : indexes) {
			GetIndexConfigResult indexConfig = zuliaWorkPool.getIndexConfig(index);
			if (view.equals(IndexSettings.Meta)) {
				ZuliaCommonCmd.printMagenta(String.format("%40s | %40s | %40s", "Index Name", "Meta Key", "Meta Value"));

				Document metaDoc = indexConfig.getIndexConfig().getMeta();
				for (String key : metaDoc.keySet()) {
					System.out.printf("%40s | %40s | %40s\n", index, key, metaDoc.get(key).toString());
				}
			}
			else if (view.equals(IndexSettings.FieldMapping)) {
				ZuliaCommonCmd.printMagenta(String.format("%40s | %40s | %120s | %20s", "Index Name", "Field Alias", "Field Mapped", "Include Self"));
				TreeMap<String, ZuliaIndex.FieldMapping> fieldMappingMap = indexConfig.getIndexConfig().getFieldMappingMap();
				for (String key : fieldMappingMap.keySet()) {
					ZuliaIndex.FieldMapping fieldMapping = fieldMappingMap.get(key);
					System.out.printf("%40s | %40s | %120s | %20s\n", index, key, fieldMapping.getFieldOrFieldPatternList(),
							fieldMappingMap.get(key).getIncludeSelf());
				}
			}
			else if (view.equals(IndexSettings.FieldConfig)) {
				ZuliaCommonCmd.printMagenta(
						String.format("%40s | %50s | %50s | %140s | %80s | %80s", "Index Name", "Stored Field Name", "Display Name", "Index As", "Facet As",
								"Sort As"));
				TreeMap<String, ZuliaIndex.FieldConfig> fieldConfigMap = indexConfig.getIndexConfig().getFieldConfigMap();
				for (String key : fieldConfigMap.keySet()) {
					ZuliaIndex.FieldConfig fieldConfig = fieldConfigMap.get(key);

					StringBuilder indexAsSummary = new StringBuilder();
					for (ZuliaIndex.IndexAs indexAs : fieldConfig.getIndexAsList()) {
						String analyzerName = indexAs.getAnalyzerName();
						String indexFieldName = indexAs.getIndexFieldName();
						if (!indexAsSummary.isEmpty()) {
							indexAsSummary.append(",");
						}
						indexAsSummary.append(indexFieldName);
						if (!analyzerName.isEmpty()) {
							indexAsSummary.append("(").append(analyzerName).append(")");
						}
					}

					StringBuilder facetAsSummary = new StringBuilder();
					for (ZuliaIndex.FacetAs facetAs : fieldConfig.getFacetAsList()) {
						String facetName = facetAs.getFacetName();
						boolean hierarchical = facetAs.getHierarchical();

						if (!facetAsSummary.isEmpty()) {
							facetAsSummary.append(",");
						}
						facetAsSummary.append(facetName);
						if (hierarchical) {
							facetAsSummary.append("(hierarchical)");
						}
					}

					StringBuilder sortAsSummary = new StringBuilder();
					for (ZuliaIndex.SortAs sortAs : fieldConfig.getSortAsList()) {
						String sortFieldName = sortAs.getSortFieldName();

						if (!sortAsSummary.isEmpty()) {
							sortAsSummary.append(",");
						}
						sortAsSummary.append(sortFieldName);

					}

					System.out.printf("%40s | %50s | %50s | %140s | %80s | %80s\n", index, key, fieldConfig.getDisplayName(), indexAsSummary, facetAsSummary,
							sortAsSummary);
				}
			}
			else if (view.equals(IndexSettings.WarmedSearches)) {
				for (ZuliaServiceOuterClass.QueryRequest warmingSearch : indexConfig.getIndexConfig().getWarmingSearches()) {
					String queryJson = JsonFormat.printer().print(warmingSearch);
					System.out.println(queryJson);
				}
			}
		}

		return CommandLine.ExitCode.OK;
	}
}
