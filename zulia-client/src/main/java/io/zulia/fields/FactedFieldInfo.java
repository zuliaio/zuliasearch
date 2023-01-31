package io.zulia.fields;

import java.lang.reflect.Field;
import java.util.*;

import static io.zulia.message.ZuliaQuery.Facet;

public class FactedFieldInfo<T> {
    private final String facetPrefix;
    private final Field field;

    public FactedFieldInfo(Field field, String facetPrefix) {
        this.facetPrefix = facetPrefix;
        this.field = field;
    }

    public String getFacetPrefix() {
        return facetPrefix;
    }

    public List<Facet> build(T object) throws IllegalArgumentException, IllegalAccessException {
        if (object != null) {
            ArrayList<Facet> list = new ArrayList<>();
            Object o = field.get(object);

            if (o != null) {

                if (o instanceof Collection<?> l) {
                    for (Object s : l) {
                        Facet.Builder lmFacetBuilder = Facet.newBuilder().setLabel(facetPrefix);
                        lmFacetBuilder.setValue(s.toString());
                        list.add(lmFacetBuilder.build());
                    }
                } else if (o.getClass().isArray()) {
                    Object[] l = (Object[]) o;
                    for (Object s : l) {
                        Facet.Builder lmFacetBuilder = Facet.newBuilder().setLabel(facetPrefix);
                        lmFacetBuilder.setValue(s.toString());
                        list.add(lmFacetBuilder.build());
                    }
                } else if (o instanceof Date d) {
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(d);

                    //TODO configurable
                    int year = cal.get(Calendar.YEAR);
                    int month = cal.get(Calendar.MONTH) + 1;
                    int day = cal.get(Calendar.DAY_OF_MONTH);

                    Facet.Builder lmFacetBuilder = Facet.newBuilder().setLabel(facetPrefix);
                    lmFacetBuilder.setValue(year + "" + month + "" + day);

                    list.add(lmFacetBuilder.build());
                } else {
                    Facet.Builder lmFacetBuilder = Facet.newBuilder().setLabel(facetPrefix);
                    lmFacetBuilder.setValue(o.toString());
                    list.add(lmFacetBuilder.build());
                }

                return list;
            }
        }

        return Collections.emptyList();

    }

}
