package com.study.mongo.lesson01_quickstart;

import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.study.mongo.lesson01_quickstart.pojo.Address;
import com.study.mongo.lesson01_quickstart.pojo.Favorites;
import com.study.mongo.lesson01_quickstart.pojo.User;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static org.springframework.data.mongodb.core.query.Update.update;

//spring Pojo的操作方式
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext.xml")
public class SpringWithMongoTest {
    @Resource
    private MongoOperations tempelate;

    @Test
    public void insertDemo() {
        User user = new User();
        user.setUsername("cang");
        user.setCountry("USA");
        user.setAge(20);
        user.setLength(1.77f);
        user.setSalary(new BigDecimal("6265.22"));

        Address address1 = new Address();
        address1.setZip("411222");
        address1.setStreet("sdfsdf");
        user.setAddress(address1);

        Favorites favorites1 = new Favorites();
        favorites1.setCites(Arrays.asList("东莞", "东京"));
        favorites1.setMovies(Arrays.asList("西游记", "一路向西"));
        user.setFavorites(favorites1);


        User user1 = new User();
        user1.setUsername("chen");
        user1.setCountry("China");
        user1.setAge(30);
        user1.setLength(1.77f);
        user1.setSalary(new BigDecimal("6885.22"));

        Address address2 = new Address();
        address2.setZip("411000");
        address2.setStreet("我的地址2");
        user1.setAddress(address2);

        Favorites favorites2 = new Favorites();
        favorites2.setCites(Arrays.asList("珠海", "东京"));
        favorites2.setMovies(Arrays.asList("东游记", "一路向东"));
        user1.setFavorites(favorites2);

        tempelate.insertAll(Arrays.asList(user, user1));
    }

    @Test
    public void testDelete() {

        //delete from users where username = ‘hash’
        DeleteResult remove = tempelate.remove(query(where("username").is("hash")), User.class);
        System.out.println("Acknowledged: " + remove.wasAcknowledged() + " DeletedCount: " + remove.getDeletedCount());

        //delete from users where age >8 and age <25
        DeleteResult remove2 = tempelate.remove(query(new Criteria().andOperator(where("age").gt(8), where("age").lt(25))), User.class);
        System.out.println("Acknowledged: " + remove2.wasAcknowledged() + " DeletedCount: " + remove2.getDeletedCount());

    }

    @Test
    public void testUpdate() {
        //update  users  set age=6 where username = 'hash' 
        UpdateResult updateFirst = tempelate.updateMulti(query(where("username").is("hash")), update("age", 6), User.class);
        System.out.println("Acknowledged: " + updateFirst.wasAcknowledged() + " ModifiedCount" + updateFirst.getModifiedCount());
        //update users  set favorites.movies add "小电影2 ", "小电影3" where favorites.cites  has "东莞"
        Query query = query(where("favorites.cites").is("东莞"));
        Update update = new Update().addToSet("favorites.movies").each("小电影2 ", "小电影3");
        UpdateResult updateMulti = tempelate.updateMulti(query, update, User.class);
        System.out.println("Acknowledged: " + updateMulti.wasAcknowledged() + " ModifiedCount" + updateMulti.getModifiedCount());
    }

    @Test
    public void testFind() {

        //select * from users  where favorites.cites has "东莞"、"东京"
        List<User> find = tempelate.find(query(where("favorites.cites").all(Arrays.asList("东莞", "东京"))), User.class);
        System.out.println(find.size());


        //select * from users  where username like '%s%' and (contry= EngLish or contry = USA)
        String regexStr = ".*c.*";
        //username like '%s%'
        Criteria regex = where("username").regex(regexStr);
        //contry= EngLish
        Criteria or1 = where("country").is("English");
        //contry= USA
        Criteria or2 = where("country").is("USA");

        Criteria or = new Criteria().orOperator(or1, or2);

        Query query = query(new Criteria().andOperator(regex, or));

        List<User> find2 = tempelate.find(query, User.class);

        System.out.println(find2.size());

    }
}

