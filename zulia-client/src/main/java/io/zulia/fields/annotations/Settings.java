package io.zulia.fields.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
public @interface Settings {
	String indexName();

	int numberOfShards();

	boolean applyUncommitedDeletes() default true;

	double requestFactor() default 2.0;

	int minSeqmentRequest() default 2;

	int idleTimeWithoutCommit() default 30;

	int shardCommitInterval() default 3200;

	double shardTolerance() default 0.05;

	int shardQueryCacheSize() default 512;

	int shardQueryCacheMaxAmount() default 256;

	boolean storeDocumentInIndex() default true;

	boolean storeDocumentInMongo() default false;

	boolean storeIndexOnDisk() default false;

}
