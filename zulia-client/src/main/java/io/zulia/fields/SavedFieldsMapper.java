package io.zulia.fields;

import io.zulia.fields.annotations.AsField;
import io.zulia.fields.annotations.DefaultSearch;
import io.zulia.fields.annotations.Embedded;
import io.zulia.fields.annotations.Faceted;
import io.zulia.fields.annotations.Indexed;
import io.zulia.fields.annotations.IndexedFields;
import io.zulia.fields.annotations.NotSaved;
import io.zulia.fields.annotations.UniqueId;
import org.bson.Document;

import java.lang.reflect.Field;
import java.util.HashSet;

public class SavedFieldsMapper<T> {

	private final Class<T> clazz;

	private final HashSet<SavedFieldInfo<T>> savedFields;

	private final HashSet<SavedEmbeddedFieldInfo<T>> savedEmbeddedFields;

	public SavedFieldsMapper(Class<T> clazz) {
		this.clazz = clazz;
		this.savedFields = new HashSet<>();
		this.savedEmbeddedFields = new HashSet<>();

	}

	public void setupField(Field f) {

		validate(f);

		String fieldName = f.getName();

		if (f.isAnnotationPresent(AsField.class)) {
			AsField as = f.getAnnotation(AsField.class);
			fieldName = as.value();
		}

		if (f.isAnnotationPresent(Embedded.class)) {
			savedEmbeddedFields.add(new SavedEmbeddedFieldInfo<>(f, fieldName));
		}
		else if (f.isAnnotationPresent(NotSaved.class)) {

		}
		else {
			savedFields.add(new SavedFieldInfo<>(f, fieldName));
		}

	}

	protected void validate(Field f) {
		if (f.isAnnotationPresent(NotSaved.class)) {
			if (f.isAnnotationPresent(IndexedFields.class) || f.isAnnotationPresent(Indexed.class) || f.isAnnotationPresent(Faceted.class) || f
					.isAnnotationPresent(UniqueId.class) || f.isAnnotationPresent(DefaultSearch.class) || f.isAnnotationPresent(Embedded.class)) {
				throw new RuntimeException(
						"Cannot use NotSaved with Indexed, Faceted, UniqueId, DefaultSearch, or Embedded on field <" + f.getName() + "> for class <" + clazz
								.getSimpleName() + ">");
			}

		}
	}

	protected Document toDocument(T object) throws Exception {
		Document document = new Document();
		for (SavedFieldInfo<T> sfi : savedFields) {
			Object o = sfi.getValue(object);
			document.put(sfi.getFieldName(), o);
		}

		for (SavedEmbeddedFieldInfo<T> sefi : savedEmbeddedFields) {
			Object o = sefi.getValue(object);
			document.put(sefi.getFieldName(), o);
		}
		return document;
	}

	protected T fromDBObject(Document savedDocument) throws Exception {
		T newInstance = clazz.newInstance();
		for (SavedFieldInfo<T> sfi : savedFields) {
			sfi.populate(newInstance, savedDocument);
		}
		for (SavedEmbeddedFieldInfo<T> sefi : savedEmbeddedFields) {
			sefi.populate(newInstance, savedDocument);
		}

		return newInstance;
	}
}
