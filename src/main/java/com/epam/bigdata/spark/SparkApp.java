package com.epam.bigdata.spark;

import com.epam.bigdata.entity.*;
import com.restfb.*;
import com.restfb.types.Event;
import com.restfb.types.Location;
import com.restfb.types.Place;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import scala.Tuple2;
import java.util.Optional;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Collections.reverseOrder;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;


/**
 * Created by Ilya_Starushchanka on 10/3/2016.
 */
public class SparkApp {

    private static final List<String> stopWords = Arrays.asList("a", "and", "for", "to", "the", "you", "in");
    private static final String UNKNOWN = "Unknown";
    private static final Pattern SPACE = Pattern.compile(" ");
    private static final String TOKEN = "EAACEdEose0cBAGdDQLcYXAtLRZBQOvH7Ja92S2aBXAllIvu7HqzWYebiyQeMsqY86yMR67pXzx5OR92vEBR7Q0rKnfZA5SeAjl3TZCz2ZBJZAXGvAK43kHZAa8KVw63jqiwZCqQWC9lRnZAON2ZAMDwizLnP6XcFxK8fZBkJcvuzXTMAZDZD";
    private static final FacebookClient facebookClient = new DefaultFacebookClient(TOKEN, Version.VERSION_2_5);
    private static final String FIELDS_NAME = "id,attending_count,place,name,description,start_time";
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static final String DEFAULT_DATE = "2000-01-01";


    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount")
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
        /*JavaPairRDD<Long, List<String>> tagsIdsPairs = tagsRDD.mapToPair(new PairFunction<String, Long, List<String>>() {
            public Tuple2<Long, List<String>> call(String line) {
                String[] parts = line.split("\\s+");
                return new Tuple2<Long, List<String>>(Long.parseLong(parts[0]), Arrays.asList(parts[1].split(",")));
            }
        });*/
        Map<Long, List<String>> tagsMap = tagsIdsPairs.collectAsMap();

        //CITIES
        Dataset<String> dataCities = spark.read().textFile(args[2]);
        String headerCities = dataCities.first();
        JavaRDD<String> citiesRDD =dataCities.filter(x -> !x.equals(headerCities)).javaRDD();
        JavaPairRDD<Integer, String> citiesIdsPairs = citiesRDD.mapToPair(new PairFunction<String, Integer, String>() {
            public Tuple2<Integer, String> call(String line) {
                String[] parts = line.split("\\s+");
                return new Tuple2<Integer, String>(Integer.parseInt(parts[0]), parts[1]);
            }
        });
        Map<Integer, String> citiesMap = citiesIdsPairs.collectAsMap();

        JavaRDD<LogsEntity> logsRDD = spark.read().textFile(args[0]).javaRDD().map(new Function<String, LogsEntity>() {
            @Override
            public LogsEntity call(String line) throws Exception {
                String[] parts = line.split("\\s+");

                LogsEntity logsEntity = new LogsEntity();

                List<String> tagsList = tagsMap.get(Long.parseLong(parts[parts.length - 2]));
                logsEntity.setTags(tagsList);

                String city = citiesMap.get(Integer.parseInt(parts[parts.length - 15]));
                logsEntity.setCity(city);

                String dateInString = parts[1].substring(0, 8);
                logsEntity.setDate(dateInString);
                return logsEntity;
            }
        });

        JavaPairRDD<CustomCityDateEntity, Set<String>> customCityDateEntityPairs = logsRDD.mapToPair(new PairFunction<LogsEntity, CustomCityDateEntity, Set<String>>() {
            @Override
            public Tuple2<CustomCityDateEntity, Set<String>> call(LogsEntity logsEntity) throws Exception {
                CustomCityDateEntity dc = new CustomCityDateEntity();
                dc.setCity(logsEntity.getCity());
                dc.setDate(logsEntity.getDate());
                return new Tuple2<>(dc, new HashSet<>(logsEntity.getTags()));
            }
        });
        JavaPairRDD<CustomCityDateEntity, Set<String>> dayCityTagsPairs = customCityDateEntityPairs.reduceByKey(new Function2<Set<String>, Set<String>, Set<String>>() {
            @Override
            public Set<String> call(Set<String> i1, Set<String> i2) {
                i1.addAll(i2);
                return i1;
            }
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

        allEventsTagEntity.collect().forEach(tagEntity -> {
            tagEntity.getAllEvents().forEach(eie -> {
                if (eie.getCity().equals(UNKNOWN)) {
                    System.out.println("TAG : " + tagEntity.getTag() + ",      CITY : " + eie.getCity() + ",      DATE : " + eie.getDate() + ",      ATTENDS : " + eie.getAttendingCount());
                }
            });
        });

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
            if (tuple._1.getCity().equals(UNKNOWN)) {
                Map<String, Long> topWords = new HashMap<String, Long>();
                if (tuple._2.getDesc() != null) {
                    List<String> words = Pattern.compile("\\W").splitAsStream(tuple._2.getDesc())
                            .filter((s -> !s.isEmpty()))
                            .filter(w -> !Pattern.compile("\\d+").matcher(w).matches())
                            .filter(w -> !stopWords.contains(w))
                            .collect(toList());
                    topWords = words.stream()
                                .map(String::toLowerCase)
                                .collect(groupingBy(java.util.function.Function.identity(), counting()))
                                .entrySet().stream()
                                .sorted(Map.Entry.comparingByValue(reverseOrder()))
                                .limit(10).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                }
                System.out.println("TAG : " + tuple._1.getTag() + ",      CITY : " + tuple._1.getCity() + ",      DATE : " + tuple._1.getDate() + ",      ATTENDS : " + tuple._2.getAttendingCount() + ",        TOKEN_MAP : " + topWords);
            }
        });


        /*Encoder<LogsEntity> logsEncoder = Encoders.bean(LogsEntity.class);

        Dataset<LogsEntity> logsDataSet = spark.createDataset(logsRDD.rdd(), logsEncoder);


        logsDataSet.show();*/

        /*JavaRDD<String> lines = spark.read().text(args[0]).javaRDD();

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) {
                return Arrays.asList(SPACE.split(s)).iterator();
            }
        });

        JavaPairRDD<String, Integer> ones = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<>(s, 1);
                    }
                });

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }*/
        spark.stop();
    }


}
