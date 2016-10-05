package com.epam.bigdata.entity;

import java.io.Serializable;

/**
 * Created by Ilya_Starushchanka on 10/5/2016.
 */
public class EventInfoEntity implements Serializable{

    private String id;
    private String name;
    private String desc;
    private int attendingCount;
    private String city;
    private String date;
    private String tag;

    public EventInfoEntity(){}

    public EventInfoEntity(String id, String name, String desc,  int attendingCount, String tag){
        this.attendingCount = attendingCount;
        this.id = id;
        this.name = name;
        this.desc = desc;
        this.tag = tag;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public int getAttendingCount() {
        return attendingCount;
    }

    public void setAttendingCount(int attendingCount) {
        this.attendingCount = attendingCount;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

}
