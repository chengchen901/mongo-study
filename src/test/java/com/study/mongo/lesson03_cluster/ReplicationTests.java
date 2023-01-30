package com.study.mongo.lesson03_cluster;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext.xml")
public class ReplicationTests {

    @Autowired
    ReplicationDemo replication;

    /**
     * 演练副本集群的可用性代码
     */
    @Test
    public void testAvailable() {
        System.out.println("run!");

        new Thread(() -> {
            while (true) {
                // 随机修改年龄
                int age = (new Random()).nextInt(800);
                replication.updateUserAge("hash", age);
                try {
                    TimeUnit.SECONDS.sleep(3);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(() -> {
            while (true) {
                // 不断读取数据
                replication.findUserByName("hash");
                try {
                    TimeUnit.SECONDS.sleep(3);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

