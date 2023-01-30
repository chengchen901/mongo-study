package com.study.mongo.lesson03_cluster;

import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.mongodb.client.model.Filters.eq;

/**
 * 主从集群示例代码
 */
@Service
public class ReplicationDemo {
    private static final Logger logger = LoggerFactory.getLogger(ReplicationDemo.class);

    private MongoDatabase dataBase;
    private MongoCollection<Document> collection;
    private MongoClient mongoClient;

    @PostConstruct
    public void init() {
        // 连接MongoDB
        mongoClient = MongoClients.create(MongoClientSettings.builder()
                .applyToClusterSettings(
                        builder ->
                                // 不要直连master，master会切换
                                builder.hosts(Arrays.asList(
                                        new ServerAddress("192.168.254.176", 28018),
                                        new ServerAddress("192.168.254.176", 28019),
                                        new ServerAddress("192.168.254.176", 28020)
                                ))
                )
                // 读写分离，读取从次节点读取
                .readPreference(ReadPreference.secondary())
                // 读策略，LOCAL
                .readConcern(ReadConcern.AVAILABLE)
                // 写策略，MAJORITY
                .writeConcern(WriteConcern.MAJORITY)
                .build());

        //MongoClient mongoClient = MongoClients.create("mongodb://192.168.120.140:28018,192.168.120.140:28019,192.168.120.140:28020/?replicaSet=rs1");


        // 选择一个数据库、集合
        dataBase = mongoClient.getDatabase("test");
        collection = dataBase.getCollection("users");

        createUser();

        // 关闭连接资源
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            delete();
            close();
        }));
    }

    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    public void createUser() {
        Document hash = new Document();
        hash.append("username", "hash");
        hash.append("country", "China");
        hash.append("age", 30);
        hash.append("lenght", 1.77f);
        hash.append("salary", new BigDecimal("8888.22"));
        Map<String, String> address = new HashMap<>();
        address.put("aCode", "411000");
        address.put("add", "我的地址2");
        Map<String, Object> favorites = new HashMap<>();
        favorites.put("movies", Arrays.asList("东游记", "一路向东"));
        favorites.put("cites", Arrays.asList("珠海", "东京"));
        hash.append("favorites", favorites);
        collection.insertMany(Arrays.asList(hash));
    }

    public void updateUserAge(String userName, int age) {
        //update  users  set age=6 where username = 'hash'
        UpdateResult updateMany = collection.updateMany(eq("username", userName),
                new Document("$set", new Document("age", age)));
        logger.info(String.valueOf(updateMany.getModifiedCount()));
    }


    public Document findUserByName(String userName) {
        Document firstDoc = collection.find(Filters.eq("username", userName)).first();
        System.out.println(firstDoc.toJson());
        return firstDoc;
    }

    public long delete() {
        DeleteResult deleteMany = collection.deleteMany(eq("username", "hash"));
        logger.info(String.valueOf(deleteMany.getDeletedCount()));
        return deleteMany.getDeletedCount();
    }

}

