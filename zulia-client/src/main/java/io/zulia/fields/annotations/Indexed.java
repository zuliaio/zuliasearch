package io.zulia.fields.annotations;

import java.lang.annotation.*;

/**
 * Specifics a field should be indexed
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
@Repeatable(IndexedFields.class)
public @interface Indexed {
    /**
     * Sets the analyzer to use to index the field
     */
    String analyzerName() default "";

    String fieldName() default "";
}
