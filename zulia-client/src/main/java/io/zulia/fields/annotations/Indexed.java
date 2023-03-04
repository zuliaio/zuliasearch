package io.zulia.fields.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifics a field should be indexed
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD })
@Repeatable(IndexedFields.class)
public @interface Indexed {
	/**
	 * Sets the analyzer to use to index the field
	 */
	String analyzerName() default "";

	String fieldName() default "";
}
