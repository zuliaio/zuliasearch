package io.zulia.fields.annotations;

import io.zulia.message.ZuliaIndex.FacetAs;

import java.lang.annotation.*;

@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
@Repeatable(FacetedFields.class)
public @interface Faceted {

    FacetAs.DateHandling dateHandling() default FacetAs.DateHandling.DATE_YYYY_MM_DD;

    String name() default "";
}
