package com.epam.bigdata.entity;

import java.util.Date;

/**
 * Created by Ilya_Starushchanka on 10/4/2016.
 */
public class LogsObject {

    private Date date;
    private long userTagsId;
    private long cityId;


    public Date getDate() {
        return date;
    }

    public void setDate(Date time) {
        this.date = time;
    }

    public long getUserTagsId() {
        return userTagsId;
    }

    public void setUserTagsId(long userTagsId) {
        this.userTagsId = userTagsId;
    }

    public long getCityId() {
        return cityId;
    }

    public void setCityId(long cityId) {
        this.cityId = cityId;
    }

}
