package io.zulia;

/**
 * Created by Payam Meyer on 8/7/17.
 *
 * @author pmeyer
 */
public interface ZuliaRESTConstants {

	int DEFAULT_SERVICE_SERVICE_PORT = 32191;
	int DEFAULT_REST_SERVICE_PORT = 32192;

	//HTTP constants
	int SUCCESS = 200;
	int BAD_REQUEST = 400;
	int NOT_FOUND = 404;
	int INTERNAL_ERROR = 500;

	String GET = "GET";
	String POST = "POST";

	String ASSOCIATED_DOCUMENTS_URL = "/associatedDocs";
	String ASSOCIATED_DOCUMENTS_ALL_FOR_ID_URL = "/associatedDocs/allForId";
	String ASSOCIATED_DOCUMENTS_METADATA_URL = "/associatedDocs/metadata";
	String QUERY_URL = "query";
	String FETCH_URL = "fetch";
	String FIELDS_URL = "fields";
	String TERMS_URL = "terms";
	String INDEX_URL = "index";
	String INDEXES_URL = "/indexes";
	String NODES_URL = "nodes";
	String STATS_URL = "stats";

	String QUERY = "q";
	String QUERY_FIELD = "qf";
	String FILTER_QUERY = "fq";
	String QUERY_JSON = "qJson";
	String ROWS = "rows";
	String ID = "id";
	String FILE_NAME = "fileName";
	String INDEX = "index";
	String FACET = "facet";
	String SORT = "sort";
	String FETCH = "fetch";
	String FIELDS = "fl";
	String PRETTY = "pretty";
	String MIN_MATCH = "mm";
	String DEBUG = "debug";
	String AMOUNT = "amount";
	String MIN_DOC_FREQ = "minDocFreq";
	String MIN_TERM_FREQ = "minTermFreq";
	String START_TERM = "startTerm";
	String END_TERM = "endTerm";
	String DEFAULT_OP = "defaultOp";
	String DRILL_DOWN = "drillDown";
	String DISMAX = "dismax";
	String DISMAX_TIE = "dismaxTie";
	String SIMILARITY = "sim";
	String START = "start";
	String ACTIVE = "active";

	String TERM_FILTER = "termFilter";
	String TERM_MATCH = "termMatch";
	String INCLUDE_TERM = "includeTerm";

	String FORMAT = "format";

	String HIGHLIGHT = "hl";

	String HIGHLIGHT_JSON = "hlJson";

	String ANALYZE_JSON = "alJson";
	String FUZZY_TERM_JSON = "fuzzyTermJson";

	String DONT_CACHE = "dontCache";
	String BATCH = "batch";
	String BATCH_SIZE = "batchSize";
	String CURSOR = "cursor";
	String TRUNCATE = "truncate";

}
