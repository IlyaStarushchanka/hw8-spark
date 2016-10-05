package com.epam.bigdata.entity;

import java.io.Serializable;

/**
 * Created by Ilya_Starushchanka on 10/5/2016.
 */
public class TagCityDateEntity implements Serializable {

    private String city;
    private String date;
    private String tag;

    public TagCityDateEntity(){}

    public TagCityDateEntity(String tag, String city, String date){
        this.tag = tag;
        this.city = city;
        this.date = date;
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


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TagCityDateEntity)) return false;

        TagCityDateEntity that = (TagCityDateEntity) o;

        if (getCity() != null ? !getCity().equals(that.getCity()) : that.getCity() != null) return false;
        if (getDate() != null ? !getDate().equals(that.getDate()) : that.getDate() != null) return false;
        return getTag() != null ? getTag().equals(that.getTag()) : that.getTag() == null;

    }

    @Override
    public int hashCode() {
        int result = getCity() != null ? getCity().hashCode() : 0;
        result = 31 * result + (getDate() != null ? getDate().hashCode() : 0);
        result = 31 * result + (getTag() != null ? getTag().hashCode() : 0);
        return result;
    }
}
