package io.zulia;

import io.zulia.message.ZuliaIndex;

public interface ZuliaFieldConstants {

	String TIMESTAMP_FIELD = "_ztsf_";

	String STORED_ID_FIELD = "_zsi_";
	String STORED_META_FIELD = "_zsmf_";
	String STORED_DOC_FIELD = "_zsdf_";

	String FIELDS_LIST_FIELD = "_zflf_";
	String CHAR_LENGTH_PREFIX = "_zcl_";
	String LIST_LENGTH_PREFIX = "_zll_";
	String SORT_SUFFIX = "_zss_";
	String FACET_STORAGE = "_zfs_";
	String FACET_STORAGE_INDIVIDUAL = ZuliaFieldConstants.FACET_STORAGE + "fi_";
	String FACET_STORAGE_GROUP = ZuliaFieldConstants.FACET_STORAGE + "fg_";

	String SCORE_FIELD = "zuliaScore";

	String ID_FIELD = "zuliaId";

	String ID_SORT_FIELD = "zuliaId";

	String FACET_PATH_DELIMITER = "/";

	String FACET_DRILL_DOWN_FIELD = "$facets";

	String LIST_LENGTH_BARS = "|||";
	String CHAR_LENGTH_BAR = "|";

	String NUMERIC_INT_SUFFIX = "_" + ZuliaIndex.FieldConfig.FieldType.NUMERIC_INT;
	String NUMERIC_LONG_SUFFIX = "_" + ZuliaIndex.FieldConfig.FieldType.NUMERIC_LONG;
	String NUMERIC_FLOAT_SUFFIX = "_" + ZuliaIndex.FieldConfig.FieldType.NUMERIC_FLOAT;
	String NUMERIC_DOUBLE_SUFFIX = "_" + ZuliaIndex.FieldConfig.FieldType.NUMERIC_DOUBLE;

	String DATE_SUFFIX = "_" + ZuliaIndex.FieldConfig.FieldType.DATE;

	String BOOL_SUFFIX = "_" + ZuliaIndex.FieldConfig.FieldType.BOOL;


}
