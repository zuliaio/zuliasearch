package io.zulia;

/**
 * Created by Payam Meyer on 8/7/17.
 *
 * @author pmeyer
 */
public interface ZuliaRESTConstants {

	int DEFAULT_SERVICE_SERVICE_PORT = 32191;
	int DEFAULT_REST_SERVICE_PORT = 32192;

	String ASSOCIATED_URL = "/associated";

	String QUERY_URL = "/query";
	String FETCH_URL = "/fetch";
	String FIELDS_URL = "/fields";
	String TERMS_URL = "/terms";
	String INDEX_URL = "/index";
	String INDEXES_URL = "/indexes";
	String NODES_URL = "/nodes";
	String STATS_URL = "/stats";

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
	String MIN_MATCH = "mm";
	String DEBUG = "debug";
	String AMOUNT = "amount";
	String MIN_DOC_FREQ = "minDocFreq";
	String MIN_TERM_FREQ = "minTermFreq";
	String START_TERM = "startTerm";
	String END_TERM = "endTerm";
	String DEFAULT_OP = "defaultOp";
	String DRILL_DOWN = "drillDown";

	String SIMILARITY = "sim";
	String START = "start";
	String ACTIVE = "active";

	String TERM_FILTER = "termFilter";
	String TERM_MATCH = "termMatch";
	String INCLUDE_TERM = "includeTerm";

	String HIGHLIGHT = "hl";

	String HIGHLIGHT_JSON = "hlJson";

	String ANALYZE_JSON = "alJson";
	String FUZZY_TERM_JSON = "fuzzyTermJson";

	String DONT_CACHE = "dontCache";
	String BATCH = "batch";
	String BATCH_SIZE = "batchSize";
	String CURSOR = "cursor";
	String TRUNCATE = "truncate";

	String UTF8_CSV = "text/csv;charset=utf-8";

	String UTF8_JSON = "application/json;charset=utf-8";

	String AND = "AND";
	String OR = "OR";

	String DESC = "DESC";
	String ASC = "ASC";
}
