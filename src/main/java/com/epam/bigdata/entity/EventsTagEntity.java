package com.epam.bigdata.entity;

import java.io.Serializable;
import java.util.List;

/**
 * Created by Ilya_Starushchanka on 10/5/2016.
 */
public class EventsTagEntity implements Serializable{

    private String tag;
    private List<EventInfoEntity> allEvents;

    public EventsTagEntity(){}

    public EventsTagEntity(List<EventInfoEntity> allEvents, String tag){
        this.allEvents = allEvents;
        this.tag = tag;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public List<EventInfoEntity> getAllEvents() {
        return allEvents;
    }

    public void setAllEvents(List<EventInfoEntity> allEvents) {
        this.allEvents = allEvents;
    }

}
