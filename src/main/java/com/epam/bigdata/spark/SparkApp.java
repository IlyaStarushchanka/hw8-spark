package com.epam.bigdata.spark;

import com.epam.bigdata.entity.LogsEntity;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
/**
 * Created by Ilya_Starushchanka on 10/3/2016.
 */
public class SparkApp {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount")
                .getOrCreate();

        //TAGS
        JavaRDD<String> tagsRDD = spark.read().textFile(args[1]).javaRDD();
        JavaPairRDD<Long, List<String>> tagsIdsPairs = tagsRDD.mapToPair(new PairFunction<String, Long, List<String>>() {
            public Tuple2<Long, List<String>> call(String line) {
                String[] parts = line.split("\\s+");
                return new Tuple2<Long, List<String>>(Long.parseLong(parts[0]), Arrays.asList(parts[1].split(",")));
            }
        });
        Map<Long, List<String>> tagsMap = tagsIdsPairs.collectAsMap();

        //CITIES
        JavaRDD<String> citiesRDD = spark.read().textFile(args[2]).javaRDD();
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


        Encoder<LogsEntity> logsEncoder = Encoders.bean(LogsEntity.class);

        Dataset<LogsEntity> logsDataSet = spark.createDataset(logsRDD.rdd(), logsEncoder);


        logsDataSet.show();

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
