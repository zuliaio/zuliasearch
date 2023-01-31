package io.zulia.server.util;

import com.mongodb.client.MongoClient;

public class MongoProvider {

    private static MongoClient mongoClient;

    public static MongoClient getMongoClient() {
        return mongoClient;
    }

    public static void setMongoClient(MongoClient mongoClient) {
        MongoProvider.mongoClient = mongoClient;
    }
}
