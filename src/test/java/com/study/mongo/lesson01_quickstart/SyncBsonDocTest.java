package com.study.mongo.lesson01_quickstart;

import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;
import java.util.function.Consumer;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.addEachToSet;

/**
 * 原生java驱动， document的同步操作方式
 */
public class SyncBsonDocTest {
    private static final Logger logger = LoggerFactory.getLogger(SyncBsonDocTest.class);

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
        collection = dataBase.getCollection("person");
    }

    @Test
    public void insertDemo() {
        Document doc1 = new Document();
        doc1.append("username", "kody");
        doc1.append("country", "USA");
        doc1.append("age", 20);
        doc1.append("lenght", 1.77f);
        doc1.append("salary", new BigDecimal("6565.22"));

        Map<String, String> address1 = new HashMap<String, String>();
        address1.put("aCode", "0000");
        address1.put("add", "xxx000");
        doc1.append("address", address1);

        Map<String, Object> favorites1 = new HashMap<String, Object>();
        favorites1.put("movies", Arrays.asList("aa", "bb"));
        favorites1.put("cites", Arrays.asList("东莞", "东京"));
        doc1.append("favorites", favorites1);

        Document doc2 = new Document();
        doc2.append("username", "hash");
        doc2.append("country", "China");
        doc2.append("age", 30);
        doc2.append("lenght", 1.77f);
        doc2.append("salary", new BigDecimal("8888.22"));
        Map<String, String> address2 = new HashMap<>();
        address2.put("aCode", "411000");
        address2.put("add", "我的地址2");
        doc1.append("address", address2);
        Map<String, Object> favorites2 = new HashMap<>();
        favorites2.put("movies", Arrays.asList("东游记", "一路向东"));
        favorites2.put("cites", Arrays.asList("珠海", "东京"));
        doc2.append("favorites", favorites2);

        collection.insertMany(Arrays.asList(doc1, doc2));
    }

    @Test
    public void testDelete() {

        //delete from users where username = ‘hash’
        DeleteResult deleteMany = collection.deleteMany(eq("username", "hash"));
        logger.info(String.valueOf(deleteMany.getDeletedCount()));

        //delete from users where age >8 and age <25
        DeleteResult deleteMany2 = collection.deleteMany(and(gt("age", 8), lt("age", 25)));
        logger.info(String.valueOf(deleteMany2.getDeletedCount()));
    }

    @Test
    public void testUpdate() {
        //update  users  set age=6 where username = 'hash'
        UpdateResult updateMany = collection.updateMany(eq("username", "hash"),
                new Document("$set", new Document("age", 6)));
        logger.info(String.valueOf(updateMany.getModifiedCount()));

        //update users  set favorites.movies add "小电影2 ", "小电影3" where favorites.cites  has "东莞"
        UpdateResult updateMany2 = collection.updateMany(eq("favorites.cites", "东莞"),
                addEachToSet("favorites.movies", Arrays.asList("小电影2 ", "小电影3")));
        logger.info(String.valueOf(updateMany2.getModifiedCount()));
    }

    @Test
    public void testFind() {
        final List<Document> ret = new ArrayList<>();
        Consumer<Document> printBlock = new Consumer<Document>() {
            @Override
            public void accept(Document t) {
                logger.info("Consumer：" + t.toJson());
                ret.add(t);
            }
        };

        Document firstDoc = collection.find().first();
        System.out.println(firstDoc.toJson());

        MongoCursor<Document> cursor = collection.find().iterator();
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }

        //select * from users  where favorites.cites has "东莞"、"东京"
        FindIterable<Document> find = collection.find(all("favorites.cites", Arrays.asList("东莞", "东京")));
        find.forEach(printBlock);
        logger.info(String.valueOf(ret.size()));
        ret.removeAll(ret);


        //select * from users  where username like '%s%' and (contry= English or contry = USA)
        String regexStr = ".*s.*";
        Bson regex = regex("username", regexStr);
        Bson or = or(eq("country", "English"), eq("country", "USA"));
        FindIterable<Document> find2 = collection.find(and(regex, or));
        find2.forEach(printBlock);
        logger.info(String.valueOf(ret.size()));
    }

    public void testIndex() {
        // 升降序索引
        collection.createIndex(Indexes.ascending("salary"));
        collection.createIndex(Indexes.descending("age"));
        collection.createIndex(Indexes.descending("age", "username"));    // 多个字段

        // 复合索引，stars字段以降序排列，name字段以升序排列
        collection.createIndex(Indexes.compoundIndex(Indexes.descending("stars"), Indexes.ascending("name")));
        // 文本索引
        collection.createIndex(Indexes.text("name"));
        // hash索引
        collection.createIndex(Indexes.hashed("_id"));

        // 地理空间索引
        // 2dsphere，支持在一个地球状球体计算几何的查询
        collection.createIndex(Indexes.geo2dsphere("contact.location"));
        // 2d，在二维平面上存储为点的数据使用索引
        collection.createIndex(Indexes.geo2d("contact.location"));
        // haystack，优化返回小面积结果的特殊指标，可提高使用平面几何的查询的性能
        IndexOptions haystackOption = new IndexOptions().bucketSize(1.0);
        collection.createIndex(
                Indexes.geoHaystack("contact.location", Indexes.ascending("stars")),
                haystackOption);

        // 索引属性
        // 唯一索引
        IndexOptions indexOptions = new IndexOptions().unique(true);
        collection.createIndex(Indexes.ascending("name", "stars"), indexOptions);

        // 部分索引
        IndexOptions partialFilterIndexOptions = new IndexOptions()
                .partialFilterExpression(Filters.exists("contact.email"));
        collection.createIndex(Indexes.descending("name", "stars"), partialFilterIndexOptions);
    }

}
