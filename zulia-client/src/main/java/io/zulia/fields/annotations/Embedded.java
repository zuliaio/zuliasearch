package io.zulia.fields.annotations;

import java.lang.annotation.*;

/**
 * Specifics a field should be indexed
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface Embedded {

}
