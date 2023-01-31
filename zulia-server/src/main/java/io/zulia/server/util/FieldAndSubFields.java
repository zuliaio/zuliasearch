package io.zulia.server.util;

import java.util.*;

public class FieldAndSubFields {

    private final Map<String, Set<String>> topLevelToChildren = new HashMap<>();
    private final Set<String> topLevelFields = new HashSet<>();

    public FieldAndSubFields(Collection<String> fields) {
        for (String field : fields) {
            int indexOfDot = field.indexOf('.');
            if (indexOfDot != -1) {
                String topLevel = field.substring(0, indexOfDot);
                topLevelFields.add(topLevel);
                topLevelToChildren.computeIfAbsent(topLevel, v -> new HashSet<>()).add(field.substring(indexOfDot + 1));
            } else {
                topLevelFields.add(field);
            }
        }
    }

    public Map<String, Set<String>> getTopLevelToChildren() {
        return topLevelToChildren;
    }

    public Set<String> getTopLevelFields() {
        return topLevelFields;
    }
}
