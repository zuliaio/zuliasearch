package io.zulia.ui.rest.persistence;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.naming.Named;

@ConfigurationProperties("mongodb.user-db")
public interface MongoUserConfiguration extends Named {

	@NonNull
	String getCollection();
}