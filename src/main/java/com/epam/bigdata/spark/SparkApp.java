package com.epam.bigdata.spark;

import com.epam.bigdata.entity.LogsObject;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
/**
 * Created by Ilya_Starushchanka on 10/3/2016.
 */
public class SparkApp {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount")
                .getOrCreate();

        JavaRDD<LogsObject> logsRDD = spark.read().format("txt").text(args[0]).javaRDD().map(new Function<String, LogsObject>() {
            @Override
            public LogsObject call(String line) throws Exception {
                String[] parts = line.split("\\s+");

                LogsObject logsObject = new LogsObject();
                logsObject.setUserTagsId(Integer.parseInt(parts[parts.length-2]));
                logsObject.setCityId(Integer.parseInt(parts[parts.length-15]));

                SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
                String dateInString = parts[1].substring(0,8);
                Date date = formatter.parse(dateInString);
                logsObject.setDate(date);
                return logsObject;
            }
        });

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
