package io.zulia.server;

import com.mongodb.MongoClient;

public class MongoProvider {

	private static MongoClient mongoClient;

	public static MongoClient getMongoClient() {
		return mongoClient;
	}

	public static void setMongoClient(MongoClient mongoClient) {
		MongoProvider.mongoClient = mongoClient;
	}
}
