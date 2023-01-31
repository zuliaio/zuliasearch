package io.zulia.fields.annotations;

import io.zulia.message.ZuliaIndex.SortAs.StringHandling;

import java.lang.annotation.*;

@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
@Repeatable(SortedFields.class)
public @interface Sorted {

    StringHandling stringHandling() default StringHandling.STANDARD;

    String fieldName() default "";
}
