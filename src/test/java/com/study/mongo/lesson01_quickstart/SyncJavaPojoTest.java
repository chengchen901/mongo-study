package com.study.mongo.lesson01_quickstart;

import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.study.mongo.lesson01_quickstart.pojo.Address;
import com.study.mongo.lesson01_quickstart.pojo.Favorites;
import com.study.mongo.lesson01_quickstart.pojo.User;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

/**
 * 在mongodb中使用Java Pojo对象<br/>
 * 需要处理java中pojo对象在bson中来回转换的问题，我们需要一个编解码器。
 */
public class SyncJavaPojoTest {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<User> collection;

    /**
     * 演示如何使用编解码器
     */
    @Before
    public void initWithCodec() {
        // 定义一个编解码器注册器
        CodecRegistry pojoCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        // 有3种方式可以使用编解码器
        // 1、在实例化MongoClient对象时进行设置
        /*MongoClientSettings settings = MongoClientSettings.builder()
                .codecRegistry(pojoCodecRegistry)
                .applyToServerSettings(builder -> builder.applyConnectionString(new ConnectionString("mongodb://192.168.254.176:27017")))
                .build();

        mongoClient = MongoClients.create(settings);*/

        mongoClient = MongoClients.create(MongoClientSettings.builder()
                .codecRegistry(pojoCodecRegistry)
                .applyToClusterSettings(
                        builder ->
                                builder.hosts(Arrays.asList(new ServerAddress("192.168.254.176", 27017)))
                ).build());

        // 2、在database的CodecRegistry方法使用
        database = mongoClient.getDatabase("test");
        database = database.withCodecRegistry(pojoCodecRegistry);

        // 3、在collection的CodecRegistry方法使用
        MongoCollection<User> docs = database.getCollection("user", User.class);
        collection = docs.withCodecRegistry(pojoCodecRegistry);
    }

    @After
    public void end() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    /**
     * 插入pojo对象
     */
//    @Test
    public void insertPojo() {
        User user = new User();
        user.setUsername("code 47");
        user.setCountry("USA");
        user.setAge(20);
        user.setLength(1.77f);
        user.setSalary(new BigDecimal("6265.22"));

        Address address1 = new Address();
        address1.setCity("New York");
        address1.setStreet("Wall 256");
        address1.setZip("004");
        user.setAddress(address1);

        Favorites favorites1 = new Favorites();
        favorites1.setCites(Arrays.asList("东莞", "东京"));
        favorites1.setMovies(Arrays.asList("西游记", "一路向西"));
        user.setFavorites(favorites1);


        User user1 = new User();
        user1.setUsername("hash");
        user1.setCountry("China");
        user1.setAge(30);
        user1.setLength(1.77f);
        user1.setSalary(new BigDecimal("6885.22"));

        Address address2 = new Address();
        address2.setCity("长沙");
        address2.setStreet("黄兴中路");
        address2.setZip("30104");
        user1.setAddress(address2);
        Favorites favorites2 = new Favorites();
        favorites2.setCites(Arrays.asList("珠海", "东京"));
        favorites2.setMovies(Arrays.asList("东游记", "一路向东"));
        user1.setFavorites(favorites2);

        collection.insertMany(Arrays.asList(user, user1));
    }

    /**
     * 查询
     */
//    @Test
    public void find() {
        List<User> results = new ArrayList<User>();
        Consumer<User> consumer = new Consumer<User>() {
            @Override
            public void accept(User t) {
                System.out.println(t);
                results.add(t);
            }
        };
        collection.find().forEach(consumer);
        System.out.println("共查询了：" + results.size() + " 条数据");

        User newYorkMan = collection.find(Filters.eq("address.city", "New York")).first();
        System.out.println("New York Man：" + newYorkMan);
    }

    /**
     * 修改
     */
//    @Test
    public void update() {
        UpdateResult result = collection.updateOne(Filters.lt("salary", new BigDecimal("6880")),
                Updates.combine(Updates.set("salary", new BigDecimal("9470.34")), Updates.set("age", "18")));
        System.out.println("修改单个文档操作，匹配到了的记录数：" + result.getMatchedCount() + " 已经修改的文档数：" + result.getModifiedCount());

        UpdateResult updateResults = collection.updateMany(
                Filters.not(Filters.eq("address.zip", null)), Updates.set("address.zip", null));
        System.out.println("修改多个文档操作，匹配到了的记录数：" + updateResults.getMatchedCount() + " 已经修改的文档数：" + updateResults.getModifiedCount());

        User user = new User();
        user.setUsername("code 47");
        user.setCountry("USA");
        user.setAge(20);
        user.setLength(1.77f);
        user.setSalary(new BigDecimal("6265.22"));

        Address address1 = new Address();
        address1.setCity("New York");
        address1.setStreet("Wall 256");
        address1.setZip("004");
        user.setAddress(address1);

        Favorites favorites1 = new Favorites();
        favorites1.setCites(Arrays.asList("东莞", "东京"));
        favorites1.setMovies(Arrays.asList("西游记", "一路向西"));
        user.setFavorites(favorites1);
        UpdateResult replaceResult = collection.replaceOne(Filters.gt("salary", new BigDecimal("6880")), user);
        System.out.println("替换文档操作，匹配到了的记录数：" + replaceResult.getMatchedCount() + " 已替换的文档数：" + replaceResult.getModifiedCount());
    }

    /**
     * 删除示例演示
     */
//    @Test
    public void delete() {
        DeleteResult delOneResult = collection.deleteOne(Filters.eq("city", "长沙"));
        System.out.println("删除单个文档操作，已删除数：" + delOneResult.getDeletedCount());

        DeleteResult delManyResult = collection.deleteMany(Filters.gt("salary", new BigDecimal(0)));
        System.out.println("删除多个文档操作，已删除数：" + delManyResult.getDeletedCount());
    }
}
