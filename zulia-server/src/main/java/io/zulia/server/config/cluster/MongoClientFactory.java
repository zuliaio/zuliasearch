package io.zulia.server.config.cluster;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.micronaut.configuration.mongo.reactive.DefaultMongoConfiguration;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Primary;
import io.micronaut.context.annotation.Requires;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import javax.inject.Singleton;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Requires(classes = MongoClient.class)
@Requires(beans = DefaultMongoConfiguration.class)
@Factory
public class MongoClientFactory {

	/**
	 * Factory method to return a client.
	 * @param configuration configuration pulled in
	 * @return mongoClient
	 */
	@Bean(preDestroy = "close")
	@Primary
	@Singleton
	MongoClient mongoClient(DefaultMongoConfiguration configuration) {
		System.out.println("Here!?");
		CodecRegistry pojoCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
				fromProviders(PojoCodecProvider.builder().automatic(true).build()));
		MongoClientSettings settings = MongoClientSettings.builder().codecRegistry(pojoCodecRegistry).build();
		MongoClient mongoClient = MongoClients.create(settings);
		return mongoClient;
	}
}