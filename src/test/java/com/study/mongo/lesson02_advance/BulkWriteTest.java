package com.study.mongo.lesson02_advance;

import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * BulkWriteTest
 */
public class BulkWriteTest {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private MongoDatabase dataBase;
    private MongoCollection<Document> collection;
    private MongoClient mongoClient;

    @Before
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

    @Test
    public void testBulkWrite() {
        // 1. 保证批量写操作顺序
        /*collection.bulkWrite(
                Arrays.asList(new InsertOneModel<>(new Document("_id", 4)),
                        new InsertOneModel<>(new Document("_id", 5)),
                        new InsertOneModel<>(new Document("_id", 6)),
                        new UpdateOneModel<>(new Document("_id", 1),
                                new Document("$set", new Document("x", 2))),
                        new DeleteOneModel<>(new Document("_id", 2)),
                        new ReplaceOneModel<>(new Document("_id", 3),
                                new Document("_id", 3).append("x", 4))));*/


        // 2. 无序的批量写操作，不保证操作的顺序
        collection.bulkWrite(
                Arrays.asList(new InsertOneModel<>(new Document("_id", 4)),
                        new InsertOneModel<>(new Document("_id", 5)),
                        new InsertOneModel<>(new Document("_id", 6)),
                        new UpdateOneModel<>(new Document("_id", 1),
                                new Document("$set", new Document("x", 2))),
                        new DeleteOneModel<>(new Document("_id", 2)),
                        new ReplaceOneModel<>(new Document("_id", 3),
                                new Document("_id", 3).append("x", 4))),
                new BulkWriteOptions().ordered(false));
    }


}
