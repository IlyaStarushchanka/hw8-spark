package com.epam.bigdata.spark;

import com.epam.bigdata.entity.*;
import com.restfb.*;
import com.restfb.types.Event;
import com.restfb.types.Location;
import com.restfb.types.Place;
import com.restfb.types.User;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import scala.Tuple2;
import java.util.Optional;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;


/**
 * Created by Ilya_Starushchanka on 10/3/2016.
 */
public class FacebookApp {

    private static final List<String> stopWords = Arrays.asList("a", "and", "for", "to", "the", "you", "in");
    private static final String UNKNOWN = "Unknown";
    private static final Pattern SPACE = Pattern.compile(" ");
    private static final String TOKEN = "EAACEdEose0cBAJVKVrKVpfxBj2ub8HiF7mAHNZANxSNEVx3nXtFRTZAg2wbg4mKP1DoEFoSYEO5bDPB2TEZCuN5ADhNjf23ZCdbrbYJRgSNynD6NWWE188lZBj9IVknck84Rt4FEwZAFlAJdOUWD5jYMxDsfLKAZC0hQwgsBGvX8gZDZD";
    private static final FacebookClient facebookClient = new DefaultFacebookClient(TOKEN, Version.VERSION_2_5);
    private static final String FIELDS_NAME = "id,attending_count,place,name,description,start_time";
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static final String DEFAULT_DATE = "2000-01-01";


    public static class CustomKryoRegistrator implements KryoRegistrator {
        public void registerClasses(Kryo kryo) {
            kryo.register(CustomCityDateEntity.class);
            kryo.register(EventAttendsEntity.class);
            kryo.register(EventInfoEntity.class);
            kryo.register(EventsTagEntity.class);
            kryo.register(LogsEntity.class);
            kryo.register(TagCityDateEntity.class);
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("FacebookSparkApp")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.kryo.registrator", CustomKryoRegistrator.class.getName())
                .getOrCreate();

        //TASK 1 ---------------------------------------------------------------------------------------------------------

        //TAGS
        Dataset<String> dataTags = spark.read().textFile(args[1]);
        String headerTags = dataTags.first();
        JavaRDD<String> tagsRDD =dataTags.filter(x -> !x.equals(headerTags)).javaRDD();
        JavaPairRDD<Long, List<String>> tagsIdsPairs = tagsRDD.mapToPair(line -> {
            String[] parts = line.split("\\s+");
            return new Tuple2<>(Long.parseLong(parts[0]), Arrays.asList(parts[1].split(",")));
        });
        Map<Long, List<String>> tagsMap = tagsIdsPairs.collectAsMap();

        //CITIES
        Dataset<String> dataCities = spark.read().textFile(args[2]);
        String headerCities = dataCities.first();
        JavaRDD<String> citiesRDD =dataCities.filter(x -> !x.equals(headerCities)).javaRDD();

        JavaPairRDD<Integer, String> citiesIdsPairs = citiesRDD.mapToPair(line -> {
            String[] parts = line.split("\\s+");
            return new Tuple2<>(Integer.parseInt(parts[0]), parts[1]);
        });
        Map<Integer, String> citiesMap = citiesIdsPairs.collectAsMap();

        JavaRDD<LogsEntity> logsRDD = spark.read().textFile(args[0]).javaRDD().map(line -> {
            String[] parts = line.split("\\s+");
            LogsEntity logsEntity = new LogsEntity();
            List<String> tagsList = tagsMap.get(Long.parseLong(parts[parts.length - 2]));
            logsEntity.setTags(tagsList);
            String city = citiesMap.get(Integer.parseInt(parts[parts.length - 15]));
            logsEntity.setCity(city);
            logsEntity.setDate(parts[1].substring(0, 8));
            return logsEntity;
        });

        JavaPairRDD<CustomCityDateEntity, Set<String>> customCityDateEntityPairs = logsRDD.mapToPair(logsEntity -> {
            CustomCityDateEntity dc = new CustomCityDateEntity();
            dc.setCity(logsEntity.getCity());
            dc.setDate(logsEntity.getDate());
            return new Tuple2<>(dc, new HashSet<>(logsEntity.getTags()));
        });

        JavaPairRDD<CustomCityDateEntity, Set<String>> dayCityTagsPairs = customCityDateEntityPairs.reduceByKey((i1,i2) -> {
            i1.addAll(i2);
            return i1;
        });

        List<Tuple2<CustomCityDateEntity, Set<String>>> output = dayCityTagsPairs.collect();
        for (Tuple2<CustomCityDateEntity,Set<String>> tuple : output) {
            System.out.println("####_" + tuple._1().getCity()+"_"+tuple._1().getDate());
            for (String tag : tuple._2()) {
                System.out.println("Tag : " + tag);
            }
        }


        //TASK 2 -------------------------------------------------------------------------------

        JavaRDD<String> uniqueTags = logsRDD
                .flatMap(logsEntity -> logsEntity.getTags().iterator())
                .distinct();

        System.out.println("Unique tags: ");
        uniqueTags.collect().forEach(tag -> System.out.println(tag));

        JavaRDD<EventsTagEntity> allEventsTagEntity = uniqueTags
                .map(tag -> {
                    Connection<Event> eventConnections = facebookClient.fetchConnection("search", Event.class,
                            Parameter.with("q", tag), Parameter.with("type", "event"), Parameter.with("fields", FIELDS_NAME));
                    List<EventInfoEntity> eventInfoEntities = new ArrayList<>();

                    eventConnections.forEach(events -> events
                            .forEach(event -> {
                                if (event != null){
                                    EventInfoEntity eventInfoEntity = new EventInfoEntity(event.getId(), event.getName(), event.getDescription(), event.getAttendingCount(), tag);
                                    String city = Optional.ofNullable(event)
                                            .map(Event::getPlace)
                                            .map(Place::getLocation)
                                            .map(Location::getCity)
                                            .orElse(UNKNOWN);

                                    eventInfoEntity.setCity(city);
                                    if (event.getStartTime() != null) {
                                        eventInfoEntity.setDate(dateFormat.format(event.getStartTime()).toString());
                                    } else {
                                        eventInfoEntity.setDate(DEFAULT_DATE);
                                    }
                                    eventInfoEntities.add(eventInfoEntity);
                                }
                            }));
                    return new EventsTagEntity(eventInfoEntities, tag);
                });

        /*allEventsTagEntity.collect().forEach(tagEntity -> {
            tagEntity.getAllEvents().forEach(eie -> {
                if (eie.getCity().equals(UNKNOWN)) {
                    System.out.println("TAG : " + tagEntity.getTag() + ",      CITY : " + eie.getCity() + ",      DATE : " + eie.getDate() + ",      ATTENDS : " + eie.getAttendingCount());
                }
            });
        });*/

        /*allEventsTagEntity.collect().forEach(tagEntity -> {
            System.out.println("Tag : " + tagEntity.getTag());
            tagEntity.getAllEvents().forEach(eie -> System.out.println("ID : " + eie.getId() + ", NAME : " + eie.getName() + ", CITY : "
                    + eie.getCity() + ", DATE : " + eie.getDate() + ", ATTENDCOUNT : " + eie.getAttendingCount()));
        });*/

        JavaRDD<EventInfoEntity> allEvents = allEventsTagEntity.flatMap(tagEvent -> tagEvent.getAllEvents()
                .iterator());


        JavaPairRDD<TagCityDateEntity, EventInfoEntity> tagCityDateSameKeys = allEvents.mapToPair(eventIE -> {
            TagCityDateEntity tagCityDateEntity = new TagCityDateEntity(eventIE.getTag(), eventIE.getCity(), eventIE.getDate());
            return new Tuple2<>(tagCityDateEntity, eventIE);
        });

        JavaPairRDD<TagCityDateEntity, EventInfoEntity> tagCityDatePairs = tagCityDateSameKeys.reduceByKey((eventIE1, eventIE2) -> {
            EventInfoEntity eventInfoEntity = new EventInfoEntity();
            eventInfoEntity.setAttendingCount(eventIE1.getAttendingCount() + eventIE2.getAttendingCount());
            eventInfoEntity.setDesc(eventIE1.getDesc() + " " + eventIE2.getDesc());
            return eventInfoEntity;
        });

        tagCityDatePairs.collect().forEach(tuple -> {
            //if (tuple._1.getCity().equals(UNKNOWN)) {
                System.out.print("TAG : " + tuple._1.getTag() + ",      CITY : " + tuple._1.getCity() + ",      DATE : " + tuple._1.getDate() + ",      ATTENDS : " + tuple._2.getAttendingCount() + ",        TOKEN_MAP : ");
                if (tuple._2.getDesc() != null) {
                    List<String> words = Pattern.compile("\\W").splitAsStream(tuple._2.getDesc())
                            .filter((s -> !s.isEmpty()))
                            .filter(w -> !Pattern.compile("\\d+").matcher(w).matches())
                            .filter(w -> !stopWords.contains(w))
                            .collect(toList());
                    words.stream()
                            .map(String::toLowerCase)
                            .collect(groupingBy(java.util.function.Function.identity(), counting()))
                            .entrySet().stream()
                            .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                            .limit(10).forEachOrdered(s -> System.out.print(s.getKey() + ":" + s.getValue() + " "));

                }
                System.out.println();
           // }
        });

        //TASK 3 --------------------------------------------------------------------------------------------------------------

        JavaRDD<EventInfoEntity> allEventsWithAttends = allEvents.map(eventIE -> {
            Connection<User> userConnections = facebookClient.fetchConnection(eventIE.getId() + "/attending", User.class, Parameter.with("limit",1000));
            EventInfoEntity eventInfoEntity = new EventInfoEntity();
            userConnections.forEach(users -> users
                    .forEach(user -> {
                        if (user != null){
                            EventAttendsEntity eventAttendsEntity = new EventAttendsEntity(user.getId(), user.getName());
                            eventInfoEntity.addToAllAttends(eventAttendsEntity);
                        }
                    }));
            return eventInfoEntity;
        });

        JavaRDD<EventAttendsEntity> allAttends = allEventsWithAttends.flatMap(eventIE -> eventIE.getAllAttends().iterator());

        JavaPairRDD<EventAttendsEntity, Integer> allAttendsWithCount = allAttends.mapToPair(attend -> new Tuple2<>(attend, 1));

        JavaPairRDD<EventAttendsEntity, Integer> allAttendsWithSameName = allAttendsWithCount.reduceByKey((i1,i2) -> i1+i2);

        JavaRDD<EventAttendsEntity> attendInformation =
                allAttendsWithSameName.map( t -> {
                    t._1.setCount(t._2);
                    return t._1;
                });

        JavaRDD<EventAttendsEntity> attendsLimit = attendInformation.sortBy(attend -> attend.getCount(), false, 1);
        Encoder<EventAttendsEntity> eventAttendsEntity = Encoders.bean(EventAttendsEntity.class);
        Dataset<EventAttendsEntity> attendsDataSet = spark.createDataset(attendsLimit.rdd(), eventAttendsEntity);

        attendsDataSet.show(20);
        spark.stop();
    }


}
