package io.zulia.client;

import com.google.common.primitives.Floats;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.*;
import io.zulia.client.command.builder.CountFacet;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.NumericStat;
import io.zulia.client.command.builder.ScoredQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.Sort;
import io.zulia.client.command.builder.StandardQuery;
import io.zulia.client.command.builder.StatFacet;
import io.zulia.client.command.builder.TermQuery;
import io.zulia.client.command.builder.VectorTopNQuery;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.AssociatedResult;
import io.zulia.client.result.FetchResult;
import io.zulia.client.result.GetFieldsResult;
import io.zulia.client.result.GetNodesResult;
import io.zulia.client.result.GetNumberOfDocsResult;
import io.zulia.client.result.GetTermsResult;
import io.zulia.client.result.SearchResult;
import io.zulia.client.result.UpdateIndexResult;
import io.zulia.doc.AssociatedBuilder;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.fields.Mapper;
import io.zulia.fields.annotations.DefaultSearch;
import io.zulia.fields.annotations.Indexed;
import io.zulia.fields.annotations.Settings;
import io.zulia.fields.annotations.UniqueId;
import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaBase.Similarity;
import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaIndex.AnalyzerSettings.Filter;
import io.zulia.message.ZuliaIndex.AnalyzerSettings.Tokenizer;
import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.message.ZuliaIndex.IndexSettings;
import io.zulia.message.ZuliaQuery;
import io.zulia.util.ResultHelper;
import org.bson.Document;

import java.io.File;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class WikiExamples {

	public void simpleClient() throws Exception {
		ZuliaWorkPool zuliaWorkPool = new ZuliaWorkPool(new ZuliaPoolConfig().addNode("someIp"));
	}

	public void fullClient() throws Exception {
		ZuliaPoolConfig zuliaPoolConfig = new ZuliaPoolConfig();
		zuliaPoolConfig.addNode("someIp");
		//optionally give ports if not default values
		//zuliaPoolConfig.addNode("localhost", 32191, 32192);

		//optional settings (default values shown)
		zuliaPoolConfig.setDefaultRetries(0);//Number of attempts to try before throwing an exception
		zuliaPoolConfig.setMaxConnections(10); //Maximum connections per server
		zuliaPoolConfig.setMaxIdle(10); //Maximum idle connections per server
		zuliaPoolConfig.setCompressedConnection(false); //Use this for WAN client connections
		zuliaPoolConfig.setPoolName(null); //For logging purposes only, null gives default of zuliaPool-n
		zuliaPoolConfig.setNodeUpdateEnabled(
				true); //Periodically update the nodes of the cluster and to enable smart routing to the correct node. Do not use this with ssh port forwarding.  This can be done manually with zuliaWorkPool.updateNodes();
		zuliaPoolConfig.setNodeUpdateInterval(10000); //Interval to update the nodes in ms
		zuliaPoolConfig.setRoutingEnabled(
				true); //enable routing indexing to the correct server, this only works if automatic node updating is enabled or it is periodically called manually.

		//create the connection pool
		ZuliaWorkPool zuliaWorkPool = new ZuliaWorkPool(zuliaPoolConfig);
	}

	public void createAnIndex(ZuliaWorkPool zuliaWorkPool) throws Exception {
		ClientIndexConfig indexConfig = new ClientIndexConfig().setIndexName("test").addDefaultSearchField("test");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD));
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("issn").indexAs(DefaultAnalyzers.LC_KEYWORD).facet());
		indexConfig.addFieldConfig(FieldConfigBuilder.createInt("an").index().sort());
		// createLong, createFloat, createDouble, createBool, createDate, createVector, createUnitVector is also available
		// or create(storedFieldName, fieldType)

		CreateIndex createIndex = new CreateIndex(indexConfig);
		zuliaWorkPool.createIndex(createIndex);
	}

	public void createIndexCustomAnalyzer(ClientIndexConfig clientIndexConfig) throws Exception {
		clientIndexConfig.addAnalyzerSetting("myAnalyzer", Tokenizer.WHITESPACE, Arrays.asList(Filter.ASCII_FOLDING, Filter.LOWERCASE), Similarity.BM25);
		clientIndexConfig.addFieldConfig(FieldConfigBuilder.create("abstract", FieldType.STRING).indexAs("myAnalyzer"));
	}

	public void createIndexCustomMetadata(ClientIndexConfig clientIndexConfig) throws Exception {
		clientIndexConfig.setMeta(new Document("category", "special").append("otherKey", 10));
	}

	public void updateIndexBasicUsage(ZuliaWorkPool zuliaWorkPool) throws Exception {
		UpdateIndex updateIndex = new UpdateIndex("someIndex");
		// ... make changes
		UpdateIndexResult updateIndexResult = zuliaWorkPool.updateIndex(updateIndex);
		// full index settings are returned after the change that can be accessed if needed
		IndexSettings fullIndexSettings = updateIndexResult.getFullIndexSettings();
	}

	public void updateIndexAddFields(ZuliaWorkPool zuliaWorkPool) throws Exception {
		UpdateIndex updateIndex = new UpdateIndex("someIndex");
		// if a field myField or otherField exists, it will be updated with these settings
		FieldConfigBuilder myField = FieldConfigBuilder.createString("myField").indexAs(DefaultAnalyzers.STANDARD).sort();
		FieldConfigBuilder otherField = FieldConfigBuilder.createString("otherField").indexAs(DefaultAnalyzers.LC_KEYWORD).sort();
		updateIndex.mergeFieldConfig(myField, otherField);
		zuliaWorkPool.updateIndex(updateIndex);
	}

	public void updateIndexReplaceFields(ZuliaWorkPool zuliaWorkPool) throws Exception {
		UpdateIndex updateIndex = new UpdateIndex("someIndex");
		// if a field myField or otherField exists, it will be updated with these settings, otherwise they are added
		FieldConfigBuilder myField = FieldConfigBuilder.createString("myField").indexAs(DefaultAnalyzers.STANDARD).sort();
		FieldConfigBuilder otherField = FieldConfigBuilder.createString("otherField").indexAs(DefaultAnalyzers.LC_KEYWORD).sort();
		updateIndex.replaceFieldConfig(myField, otherField);
		zuliaWorkPool.updateIndex(updateIndex);
	}

	public void updateIndexRemoveFields(ZuliaWorkPool zuliaWorkPool) throws Exception {
		UpdateIndex updateIndex = new UpdateIndex("someIndex");
		// removes the stored field with name myField if it exists
		updateIndex.removeFieldConfigByStoredName(List.of("myField"));
		zuliaWorkPool.updateIndex(updateIndex);
	}

	public void updateIndexMergeAnalyzer(ZuliaWorkPool zuliaWorkPool) throws Exception {
		UpdateIndex updateIndex = new UpdateIndex("someIndex");
		// if an analyzer custom or mine exists, it will be updated with these settings, otherwise they are added
		ZuliaIndex.AnalyzerSettings custom = ZuliaIndex.AnalyzerSettings.newBuilder().setName("custom").addFilter(Filter.LOWERCASE).build();
		ZuliaIndex.AnalyzerSettings mine = ZuliaIndex.AnalyzerSettings.newBuilder().setName("mine").addFilter(Filter.LOWERCASE).addFilter(Filter.BRITISH_US)
				.build();
		updateIndex.mergeAnalyzerSettings(custom, mine);
	}

	public void updateIndexReplaceAnalyzer(ZuliaWorkPool zuliaWorkPool) throws Exception {
		UpdateIndex updateIndex = new UpdateIndex("someIndex");
		// replaces all analyzers with the two custom analyzers given
		ZuliaIndex.AnalyzerSettings custom = ZuliaIndex.AnalyzerSettings.newBuilder().setName("custom").addFilter(Filter.LOWERCASE).build();
		ZuliaIndex.AnalyzerSettings mine = ZuliaIndex.AnalyzerSettings.newBuilder().setName("mine").addFilter(Filter.LOWERCASE).addFilter(Filter.BRITISH_US)
				.build();
		updateIndex.replaceAnalyzerSettings(custom, mine);
	}

	public void updateIndexRemoveAnalyzer(ZuliaWorkPool zuliaWorkPool) throws Exception {
		UpdateIndex updateIndex = new UpdateIndex("someIndex");
		// removes the analyzer field with name myCustomOne if it exists
		updateIndex.removeAnalyzerSettingsByName(List.of("myCustomOne"));
		zuliaWorkPool.updateIndex(updateIndex);
	}

	public void updateIndexMergeMeta(ZuliaWorkPool zuliaWorkPool) throws Exception {
		UpdateIndex updateIndex = new UpdateIndex("someIndex");
		// replaces key someKey with value 5 and otherKey with value "a string" if they exist, otherwise add they to the metadata (putAll with new metadata)
		updateIndex.mergeMetadata(new Document().append("someKey", 5).append("otherKey", "a string"));
		zuliaWorkPool.updateIndex(updateIndex);
	}

	public void updateIndexReplaceMeta(ZuliaWorkPool zuliaWorkPool) throws Exception {
		UpdateIndex updateIndex = new UpdateIndex("someIndex");
		// replaces metadata document with the document below
		updateIndex.replaceMetadata(new Document().append("stuff", "for free"));
		zuliaWorkPool.updateIndex(updateIndex);
	}

	public void updateIndexRemoveMeta(ZuliaWorkPool zuliaWorkPool) throws Exception {
		UpdateIndex updateIndex = new UpdateIndex("someIndex");
		// replaces metadata document with the document below
		updateIndex.removeMetadataByKey(List.of("oneKey", "twoKey", "redKey", "blueKey"));
		zuliaWorkPool.updateIndex(updateIndex);
	}

	public void deleteIndex(ZuliaWorkPool zuliaWorkPool) throws Exception {
		zuliaWorkPool.deleteIndex("myIndex");
	}

	public void deleteIndexAndAssociatedFiles(ZuliaWorkPool zuliaWorkPool) throws Exception {
		DeleteIndex deleteIndex = new DeleteIndex("myIndex").setDeleteAssociated(true);
		zuliaWorkPool.deleteIndex(deleteIndex);
	}

	public void storingBson(ZuliaWorkPool zuliaWorkPool) throws Exception {
		Document document = new Document();
		document.put("id", "myid222");
		document.put("title", "Magic Java Beans");
		document.put("issn", "4321-4321");

		Store store = new Store("myid222", "myIndexName");

		ResultDocBuilder resultDocumentBuilder = new ResultDocBuilder().setDocument(document);
		//optional metadata document
		resultDocumentBuilder.setMetadata(new Document().append("test1", "val1").append("test2", "val2"));
		store.setResultDocument(resultDocumentBuilder);

		zuliaWorkPool.store(store);
	}

	public void storingAssociatedDocument(ZuliaWorkPool zuliaWorkPool) throws Exception {
		AssociatedBuilder associatedBuilder = new AssociatedBuilder();
		associatedBuilder.setFilename("myfile2.txt");
		// either set as text
		associatedBuilder.setDocument("Some Text3");
		// or as bytes
		associatedBuilder.setDocument(new byte[] { 0, 1, 2, 3 });
		associatedBuilder.setMetadata(new Document().append("mydata", "myvalue2").append("sometypeinfo", "text file2"));

		//can be part of the same store request as the document
		Store store = new Store("myid123", "someIndex");

		//multiple associated documented can be added at once
		store.addAssociatedDocument(associatedBuilder);

		zuliaWorkPool.store(store);
	}

	public void storeLargeAssociated(ZuliaWorkPool zuliaWorkPool) throws Exception {
		StoreLargeAssociated storeLargeAssociated = new StoreLargeAssociated("myid333", "myIndexName", "myfilename", new File("/tmp/myFile"));
		zuliaWorkPool.storeLargeAssociated(storeLargeAssociated);
	}

	public void fetchDocument(ZuliaWorkPool zuliaWorkPool) throws Exception {
		FetchDocument fetchDocument = new FetchDocument("myid222", "myIndex");

		FetchResult fetchResult = zuliaWorkPool.fetch(fetchDocument);

		if (fetchResult.hasResultDocument()) {
			Document document = fetchResult.getDocument();

			//Get optional Meta
			Document meta = fetchResult.getMeta();
		}
	}

	public void fetchAllAssociated(ZuliaWorkPool zuliaWorkPool) throws Exception {
		FetchAllAssociated fetchAssociated = new FetchAllAssociated("myid123", "myIndexName");

		FetchResult fetchResult = zuliaWorkPool.fetch(fetchAssociated);

		if (fetchResult.hasResultDocument()) {
			Document object = fetchResult.getDocument();

			//Get optional metadata
			Document meta = fetchResult.getMeta();
		}

		for (AssociatedResult ad : fetchResult.getAssociatedDocuments()) {
			//use correct function for document type
			String text = ad.getDocumentAsUtf8();
			// OR
			byte[] documentAsBytes = ad.getDocumentAsBytes();

			//get optional metadata
			Document meta = ad.getMeta();

			String filename = ad.getFilename();

		}
	}

	public void fetchAssociated(ZuliaWorkPool zuliaWorkPool) throws Exception {
		FetchAssociated fetchAssociated = new FetchAssociated("myid123", "myIndexName", "myfile2");

		FetchResult fetchResult = zuliaWorkPool.fetch(fetchAssociated);

		AssociatedResult ad = fetchResult.getFirstAssociatedDocument();
		//use correct function for document type
		String text = ad.getDocumentAsUtf8();
		// OR
		byte[] documentAsBytes = ad.getDocumentAsBytes();

		//get optional metadata
		Document meta = ad.getMeta();

		String filename = ad.getFilename();
	}

	public void fetchLargeAssociated(ZuliaWorkPool zuliaWorkPool) throws Exception {
		FetchLargeAssociated fetchLargeAssociated = new FetchLargeAssociated("myid333", "myIndexName", "myfilename", new File("/tmp/myFetchedFile"));
		zuliaWorkPool.fetchLargeAssociated(fetchLargeAssociated);
	}

	public void simpleQueryWithIdsOnly(ZuliaWorkPool zuliaWorkPool) throws Exception {
		Search search = new Search("myIndexName").setAmount(10);
		search.addQuery(new ScoredQuery("issn:1234-1234 AND title:special"));
		search.setResultFetchType(ZuliaQuery.FetchType.NONE); // just return the score and unique id

		SearchResult searchResult = zuliaWorkPool.search(search);

		long totalHits = searchResult.getTotalHits();

		System.out.println("Found <" + totalHits + "> hits");
		for (ZuliaQuery.ScoredResult sr : searchResult.getResults()) {
			System.out.println("Matching document <" + sr.getUniqueId() + "> with score <" + sr.getScore() + ">");
		}
	}

	public void simpleQueryWithFullDoc(ZuliaWorkPool zuliaWorkPool) throws Exception {

		Search search = new Search("myIndexName").setAmount(10);
		search.addQuery(new ScoredQuery("issn:1234-1234 AND title:special"));
		search.setResultFetchType(ZuliaQuery.FetchType.FULL); //return the full bson document that was stored

		SearchResult searchResult = zuliaWorkPool.search(search);

		long totalHits = searchResult.getTotalHits();

		System.out.println("Found <" + totalHits + "> hits");
		for (Document document : searchResult.getDocuments()) {
			System.out.println("Matching document <" + document + ">");
		}
	}

	public void multipleIndexes(ZuliaWorkPool zuliaWorkPool) throws Exception {
		Search search = new Search("myIndexName", "myOtherIndex").setAmount(10);
		search.addQuery(new ScoredQuery("issn:1234-1234 AND title:special"));

		SearchResult searchResult = zuliaWorkPool.search(search);

		long totalHits = searchResult.getTotalHits();

		System.out.println("Found <" + totalHits + "> hits");
		for (ZuliaQuery.ScoredResult sr : searchResult.getResults()) {
			Document doc = ResultHelper.getDocumentFromScoredResult(sr);
			System.out.println("Matching document <" + sr.getUniqueId() + "> with score <" + sr.getScore() + "> from index <" + sr.getIndexName() + ">");
			System.out.println(" full document <" + doc + ">");
		}
	}

	public void sorting(ZuliaWorkPool zuliaWorkPool) throws Exception {
		Search search = new Search("myIndexName").setAmount(100);
		search.addQuery(new FilterQuery("title:(brown AND bear)"));
		// can add multiple sorts with ascending or descending (default ascending)
		// can also specify whether missing values are returned first or last (default missing first)
		search.addSort(new Sort("year").descending());
		search.addSort(new Sort("journal").ascending().missingLast());
		SearchResult searchResult = zuliaWorkPool.search(search);
	}

	// query fields set the search field used when one is not given for a term
	// if query fields are not set on the query and a term is not qualified, the default search fields on the index will be used
	public void queryFields(ZuliaWorkPool zuliaWorkPool) throws Exception {
		Search search = new Search("myIndexName").setAmount(100);

		// search for lung in title,abstract AND cancer in title,abstract AND treatment in title
		search.addQuery(new ScoredQuery("lung cancer title:treatment").addQueryFields("title", "abstract").setDefaultOperator(ZuliaQuery.Query.Operator.AND));

		// search for lung in default index fields OR cancer in default index fields
		// OR is the default operator unless set
		search.addQuery(new ScoredQuery("lung cancer"));
	}

	public void filterQueries(ZuliaWorkPool zuliaWorkPool) throws Exception {
		Search search = new Search("myIndexName").setAmount(100);
		// include only years 2020 forward
		search.addQuery(new FilterQuery("year:[2020 TO *]"));
		// require both terms to be matched in either the title or abstract
		search.addQuery(new FilterQuery("cheetah cub").setDefaultOperator(ZuliaQuery.Query.Operator.AND).addQueryFields("title", "abstract"));
		// require two out of the three terms in the abstract
		search.addQuery(new FilterQuery("sleep play run").setMinShouldMatch(2).addQueryField("abstract"));
		// exclude the journal nature
		search.addQuery(new FilterQuery("journal:Nature").exclude());
		SearchResult searchResult = zuliaWorkPool.search(search);
	}

	public void termQueries(ZuliaWorkPool zuliaWorkPool) throws Exception {
		Search search = new Search("myIndexName").setAmount(100);

		// search for the terms 1,2,3,4 in the field id
		search.addQuery(new TermQuery("id").addTerms("1", "2", "3", "4"));

		SearchResult searchResult = zuliaWorkPool.search(search);
	}

	public void vectorQueriesAndIndexing(ZuliaWorkPool zuliaWorkPool) throws Exception {

		// create an index with add field config
		ClientIndexConfig indexConfig = new ClientIndexConfig();

		// call createVector or createUnitVector depending on if the vector is unit normalized
		indexConfig.addFieldConfig(FieldConfigBuilder.createUnitVector("v").index());
		// ...
		indexConfig.setIndexName("vectorTestIndex");
		// also can could updateIndex with mergeFieldConfig to add vector field to existing index
		zuliaWorkPool.createIndex(indexConfig);

		// store some documents with a vector field
		Document mongoDocument = new Document();
		float[] vector = new float[] { 0, 0, 0.70710678f, 0.70710678f };
		mongoDocument.put("v", Floats.asList(vector));
		Store s = new Store("someId", "vectorTestIndex").setResultDocument(mongoDocument);
		zuliaWorkPool.store(s);

		Search search = new Search("vectorTestIndex").setAmount(100);
		// returns the top 3 documents closest to [1.0,0,0,0] in the field 3
		search.addQuery(new VectorTopNQuery(new float[] { 1.0f, 0.0f, 0.0f, 0.0f }, 3, "v"));

		SearchResult searchResult = zuliaWorkPool.search(search);

	}

	public void vectorQueriesPreFilter(ZuliaWorkPool zuliaWorkPool) throws Exception {
		Search search = new Search("vectorTestIndex").setAmount(100);
		// filters for blue in the description then returns the top 3 documents closest to [1.0,0,0,0] in the field v
		StandardQuery descriptionQuery = new FilterQuery("blue").addQueryField("description");
		search.addQuery(new VectorTopNQuery(new float[] { 1.0f, 0.0f, 0.0f, 0.0f }, 3, "v").addPreFilterQuery(descriptionQuery));
	}

	public void vectorQueriesPostFilter(ZuliaWorkPool zuliaWorkPool) throws Exception {
		Search search = new Search("vectorTestIndex").setAmount(100);
		// returns the top 3 documents closest to [1.0,0,0,0] in the field v, then filters for red in the description (possible less than 3 now)
		search.addQuery(new VectorTopNQuery(new float[] { 1.0f, 0.0f, 0.0f, 0.0f }, 3, "v"));
		search.addQuery(new FilterQuery("red").addQueryField("description"));
	}

	public void countFacets(ZuliaWorkPool zuliaWorkPool) throws Exception {
		// Can set number of documents to return to 0 or omit setAmount unless you want the documents at the same time
		Search search = new Search("myIndexName").setAmount(0);

		search.addCountFacet(new CountFacet("issn").setTopN(20));

		SearchResult searchResult = zuliaWorkPool.search(search);
		for (ZuliaQuery.FacetCount fc : searchResult.getFacetCounts("issn")) {
			System.out.println("Facet <" + fc.getFacet() + "> with count <" + fc.getCount() + ">");
		}
	}

	public void countFacetDrillDown(ZuliaWorkPool zuliaWorkPool) throws Exception {
		Search search = new Search("myIndexName").setAmount(100);
		search.addFacetDrillDown("issn", "1111-1111");
		SearchResult searchResult = zuliaWorkPool.search(search);
	}

	public void numericStat(ZuliaWorkPool zuliaWorkPool) throws Exception {
		Search search = new Search("myIndexName").setAmount(100);
		search.addStat(new NumericStat("pubYear"));
		SearchResult searchResult = zuliaWorkPool.search(search);
	}

	public void statFacet(ZuliaWorkPool zuliaWorkPool) throws Exception {
		Search search = new Search("myIndexName").setAmount(100);
		search.addStat(new StatFacet("authorCount", "journalName"));
		SearchResult searchResult = zuliaWorkPool.search(search);
	}

	public void secondPageCursor(ZuliaWorkPool zuliaWorkPool) throws Exception {
		Search search = new Search("myIndexName");
		search.setAmount(100);
		search.addQuery(new ScoredQuery("issn:1234-1234 AND title:special"));

		// on a changing index a sort on  is necessary
		// it can be sort on another field AND id as well
		search.addSort(new Sort("id"));

		SearchResult firstResult = zuliaWorkPool.search(search);

		search.setLastResult(firstResult);

		SearchResult secondResult = zuliaWorkPool.search(search);
	}

	public void allPagesCursor(ZuliaWorkPool zuliaWorkPool) throws Exception {
		Search search = new Search("myIndexName");
		search.setAmount(100); //this will be the page size
		search.addQuery(new ScoredQuery("issn:1234-1234 AND title:special"));

		// on a changing index a sort on  is necessary
		// it can be sort on another field AND id as well
		search.addSort(new Sort("id"));

		//option 1 - requires fetch type full (default)
		zuliaWorkPool.searchAllAsDocument(search, document -> {
			// do something with mongo bson document
		});

		//variation 2 - when score is needed, searching multiple indexes and index name is needed, or fetch type is NONE/META
		zuliaWorkPool.searchAllAsScoredResult(search, scoredResult -> {
			System.out.println(scoredResult.getUniqueId() + " has score " + scoredResult.getScore() + " for index " + scoredResult.getIndexName());
			// if result fetch type is full (default)
			Document document = ResultHelper.getDocumentFromScoredResult(scoredResult);
		});

		//variation 3 - each page is a returned as a search result.  less convenient but gives access to total hits
		zuliaWorkPool.searchAll(search, searchResult -> {
			System.out.println("There are " + searchResult.getTotalHits());

			// variation 3a - requires fetch type full (default)
			for (Document document : searchResult.getDocuments()) {

			}

			// variation 3b - when score is needed, searching multiple indexes and index name is needed, or fetch type is NONE/META
			for (ZuliaQuery.ScoredResult result : searchResult.getResults()) {

			}
		});
	}

	public void deleteFromIndex(ZuliaWorkPool zuliaWorkPool) throws Exception {
		//Deletes the document from the index but not any associated documents
		DeleteFromIndex deleteFromIndex = new DeleteFromIndex("myid111", "myIndexName");
		zuliaWorkPool.delete(deleteFromIndex);
	}

	public void deleteCompletely(ZuliaWorkPool zuliaWorkPool) throws Exception {
		//Deletes the result document, the index documents and all associated documents associated with an id
		DeleteFull deleteFull = new DeleteFull("myid123", "myIndexName");
		zuliaWorkPool.delete(deleteFull);
	}

	public void deleteSingleAssociated(ZuliaWorkPool zuliaWorkPool) throws Exception {
		//Removes a single associated document with the unique id and filename given
		DeleteAssociated deleteAssociated = new DeleteAssociated("myid123", "myIndexName", "myfile2");
		zuliaWorkPool.delete(deleteAssociated);
	}

	public void deleteAllAssocaited(ZuliaWorkPool zuliaWorkPool) throws Exception {
		DeleteAllAssociated deleteAllAssociated = new DeleteAllAssociated("myid123", "myIndexName");
		zuliaWorkPool.delete(deleteAllAssociated);
	}

	public void getCurrentDocuemntCountForIndex(ZuliaWorkPool zuliaWorkPool) throws Exception {
		GetNumberOfDocsResult result = zuliaWorkPool.getNumberOfDocs("myIndexName");
		System.out.println(result.getNumberOfDocs());
	}

	public void getFieldsForIndex(ZuliaWorkPool zuliaWorkPool) throws Exception {
		GetFieldsResult result = zuliaWorkPool.getFields(new GetFields("myIndexName"));
		System.out.println(result.getFieldNames());
	}

	public void getTermsForField(ZuliaWorkPool zuliaWorkPool) throws Exception {
		GetTermsResult getTermsResult = zuliaWorkPool.getTerms(new GetTerms("myIndexName", "title"));
		for (ZuliaBase.Term term : getTermsResult.getTerms()) {
			System.out.println(term.getValue() + ": " + term.getDocFreq());
		}
	}

	public void getClusterNodes(ZuliaWorkPool zuliaWorkPool) throws Exception {
		GetNodesResult getNodesResult = zuliaWorkPool.getNodes();
		for (ZuliaBase.Node node : getNodesResult.getNodes()) {
			System.out.println(node);
		}
	}

	public void asyncExample(ZuliaWorkPool zuliaWorkPool) throws Exception {
		Executor executor = Executors.newCachedThreadPool();

		Search search = new Search("myIndexName").setAmount(10);

		ListenableFuture<SearchResult> resultFuture = zuliaWorkPool.searchAsync(search);

		Futures.addCallback(resultFuture, new FutureCallback<>() {
			@Override
			public void onSuccess(SearchResult result) {

			}

			@Override
			public void onFailure(Throwable t) {

			}
		}, executor);
	}

	@Settings(indexName = "wikipedia", numberOfShards = 16, shardCommitInterval = 6000)
	public class Article {

		public Article() {

		}

		@UniqueId
		private String id;

		@Indexed(analyzerName = DefaultAnalyzers.STANDARD)
		private String title;

		@Indexed
		private Integer namespace;

		@DefaultSearch
		@Indexed(analyzerName = DefaultAnalyzers.STANDARD)
		private String text;

		private Long revision;

		@Indexed
		private Integer userId;

		@Indexed(analyzerName = DefaultAnalyzers.STANDARD)
		private String user;

		@Indexed
		private Date revisionDate;

		//Getters and Setters
		//....
	}

	public void creatingIndexAnnotatedClass(ZuliaWorkPool zuliaWorkPool) throws Exception {
		Mapper<Article> mapper = new Mapper<>(Article.class);
		zuliaWorkPool.createIndex(mapper.createOrUpdateIndex());
	}

	public void storingAnObjectWithMapper(ZuliaWorkPool zuliaWorkPool, Mapper<Article> mapper) throws Exception {
		Article article = new Article();
		//...
		Store store = mapper.createStore(article);
		zuliaWorkPool.store(store);
	}

	public void queryingWithMapper(ZuliaWorkPool zuliaWorkPool, Mapper<Article> mapper) throws Exception {
		Search search = new Search("wikipedia").setAmount(10);
		search.addQuery(new ScoredQuery("title:technology"));

		SearchResult searchResult = zuliaWorkPool.search(search);
		List<Article> articles = searchResult.getMappedDocuments(mapper);
	}

}
