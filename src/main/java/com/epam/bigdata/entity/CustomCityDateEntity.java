package com.epam.bigdata.entity;

import java.io.Serializable;
import java.util.Set;

/**
 * Created by Ilya_Starushchanka on 10/4/2016.
 */
public class CustomCityDateEntity implements Serializable{

    private String date;
    private String city;

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CustomCityDateEntity)) return false;

        CustomCityDateEntity that = (CustomCityDateEntity) o;

        if (getDate() != null ? !getDate().equals(that.getDate()) : that.getDate() != null) return false;
        return getCity() != null ? getCity().equals(that.getCity()) : that.getCity() == null;

    }

    @Override
    public int hashCode() {
        int result = getDate() != null ? getDate().hashCode() : 0;
        result = 31 * result + (getCity() != null ? getCity().hashCode() : 0);
        return result;
    }
}
