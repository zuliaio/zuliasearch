package io.zulia.fields.annotations;

import io.zulia.message.ZuliaIndex;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Index-only default used when the stored path resolves to nothing. The value is parsed against the declared Java
 * field type, with Date as a supported date string such as 1970-01-01.
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD })
public @interface DefaultValue {

	String value();

	/**
	 * EACH_ELEMENT also fills null or absent list elements, WHOLE_VALUE only a whole missing value.
	 */
	ZuliaIndex.DefaultValue.Fill fill() default ZuliaIndex.DefaultValue.Fill.WHOLE_VALUE;
}
