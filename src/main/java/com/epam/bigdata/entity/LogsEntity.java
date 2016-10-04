package com.epam.bigdata.entity;

import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * Created by Ilya_Starushchanka on 10/4/2016.
 */
public class LogsEntity {

    private String date;
    private List<String> tags;
    private String city;


    public String getDate() {
        return date;
    }

    public void setDate(String time) {
        this.date = time;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

}
