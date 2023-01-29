package com.study.mongo.lesson01_quickstart.pojo;

public class Address {
    private String street;
    private String city;
    private String zip;

    public Address() {
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(final String street) {
        this.street = street;
    }

    public String getCity() {
        return city;
    }

    public void setCity(final String city) {
        this.city = city;
    }

    public String getZip() {
        return zip;
    }

    public void setZip(final String zip) {
        this.zip = zip;
    }

    @Override
    public String toString() {
        return "Address [street=" + street + ", city=" + city + ", zip=" + zip + "]";
    }

}
