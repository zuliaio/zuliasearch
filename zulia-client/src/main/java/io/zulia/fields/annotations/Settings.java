package io.zulia.fields.annotations;

import java.lang.annotation.*;

@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface Settings {
    String indexName();

    int numberOfShards();


    double requestFactor() default 2.0;

    int minSeqmentRequest() default 2;

    int idleTimeWithoutCommit() default 30;

    int shardCommitInterval() default 3200;

    double shardTolerance() default 0.05;

    int shardQueryCacheSize() default 512;

    int shardQueryCacheMaxAmount() default 256;


}
