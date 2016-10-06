package com.epam.bigdata.entity;

import java.io.Serializable;

/**
 * Created by Ilya_Starushchanka on 10/6/2016.
 */
public class EventAttendsEntity implements Serializable{

    private String name;
    private String id;
    private int count;

    public EventAttendsEntity(){}

    public EventAttendsEntity(String id, String name){
        this.id = id;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventAttendsEntity)) return false;

        EventAttendsEntity that = (EventAttendsEntity) o;

        if (getCount() != that.getCount()) return false;
        if (getName() != null ? !getName().equals(that.getName()) : that.getName() != null) return false;
        return getId() != null ? getId().equals(that.getId()) : that.getId() == null;

    }

    @Override
    public int hashCode() {
        int result = getName() != null ? getName().hashCode() : 0;
        result = 31 * result + (getId() != null ? getId().hashCode() : 0);
        result = 31 * result + getCount();
        return result;
    }
}
