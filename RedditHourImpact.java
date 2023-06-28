package com.RUSpark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;

public class RedditHourImpact {

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: RedditHourImpact <file>");
            System.exit(1);
        }

        String InputPath = args[0];

        SparkSession spark = SparkSession
                .builder()
                .appName("RedditHourImpact")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        JavaRDD<String> lines = jsc.textFile(InputPath);

        JavaPairRDD<Integer, Integer> impactPairs = lines
                .flatMap(line -> Arrays.asList(line.split("\n")).iterator())
                .mapToPair(line -> {
                    String[] columns = line.split(",");
                    long unixtime = Long.parseLong(columns[1].trim());
                    int upvotes = Integer.parseInt(columns[4].trim());
                    int downvotes = Integer.parseInt(columns[5].trim());
                    int comments = Integer.parseInt(columns[6].trim());
                    int impact = upvotes + downvotes + comments;

                    LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(unixtime), ZoneId.of("America/New_York"));
                    int hour = dateTime.getHour();

                    return new Tuple2<>(hour, impact);
                });

        JavaPairRDD<Integer, Integer> hourImpact = impactPairs.reduceByKey((x, y) -> x + y);

        hourImpact.foreach(result -> System.out.println(result._1 + " " + result._2));

        spark.stop();
    }
}
