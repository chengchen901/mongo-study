package com.study.mongo.lesson02_advance;

import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.function.Consumer;

/**
 * AggregateTest
 */
public class AggregateTest {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private MongoDatabase dataBase;
    private MongoCollection<Document> collection;
    private MongoClient mongoClient;

    public void init() {
        // 连接MongoDB
        mongoClient = MongoClients.create(MongoClientSettings.builder()
                .applyToClusterSettings(
                        builder ->
                                builder.hosts(Arrays.asList(new ServerAddress("192.168.254.176", 27017)))
                ).build());

        //mongoClient = MongoClients.create("mongodb://192.168.254.176:27017");

        // 选择一个数据库、集合
        dataBase = mongoClient.getDatabase("test");
        collection = dataBase.getCollection("test");
    }

    public void test() {
        Consumer<Document> printBlock = new Consumer<Document>() {
            @Override
            public void accept(Document t) {
                logger.info(t.toJson());
            }
        };

        collection.aggregate(
                Arrays.asList(
                        Aggregates.match(Filters.eq("categories", "Bakery")),
                        Aggregates.group("$stars", Accumulators.sum("count", 1))
                )
        ).forEach(printBlock);

        collection.aggregate(
                Arrays.asList(
                        Aggregates.project(
                                Projections.fields(
                                        Projections.excludeId(),
                                        Projections.include("name"),
                                        Projections.computed(
                                                "firstCategory",
                                                new Document("$arrayElemAt", Arrays.asList("$categories", 0))
                                        )
                                )
                        )
                )
        ).forEach(printBlock);
    }
}
