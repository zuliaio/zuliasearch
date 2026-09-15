package io.zulia.fields.annotations;

import io.zulia.message.ZuliaIndex.FieldConfig.MalformedValueHandling;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Handling for a stored value that cannot be parsed for the field type: FAIL, SKIP or USE_DEFAULT.
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD })
public @interface OnMalformed {

	MalformedValueHandling value();
}
