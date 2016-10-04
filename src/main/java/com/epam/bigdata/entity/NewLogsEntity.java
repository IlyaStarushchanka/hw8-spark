package com.epam.bigdata.entity;

import java.util.Set;

/**
 * Created by Ilya_Starushchanka on 10/4/2016.
 */
public class NewLogsEntity {

    private CustomCityDateEntity cityDateEntity;
    private Set<String> tags;

    public Set<String> getTags() {
        return tags;
    }

    public void setTags(Set<String> tags) {
        this.tags = tags;
    }

    public CustomCityDateEntity getCityDateEntity() {
        return cityDateEntity;
    }

    public void setCityDateEntity(CustomCityDateEntity cityDateEntity) {
        this.cityDateEntity = cityDateEntity;
    }
}
