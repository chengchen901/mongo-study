package com.study.mongo.lesson01_quickstart.pojo;

import org.bson.types.ObjectId;

public class Person {
    private ObjectId id;
    private String name;
    private int age;
    private Address address;

    public Person() {
    }

    public ObjectId getId() {
        return id;
    }

    public void setId(final ObjectId id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(final int age) {
        this.age = age;
    }

    public Address getAddress() {
        return address;
    }

    public void setAddress(final Address address) {
        this.address = address;
    }
}
