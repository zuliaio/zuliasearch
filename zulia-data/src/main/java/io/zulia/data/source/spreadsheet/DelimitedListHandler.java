package io.zulia.data.source.spreadsheet;

import java.util.Collection;
import java.util.List;

public interface DelimitedListHandler {

	<T> List<T> cellValueToList(Class<T> clazz, String cellValue);

	String collectionToCellValue(Collection<?> collection);
}
