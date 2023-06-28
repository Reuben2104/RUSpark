package com.RUSpark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;

public class RedditPhotoImpact {

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: RedditPhotoImpact <file>");
            System.exit(1);
        }

        String InputPath = args[0];

        SparkSession spark = SparkSession
                .builder()
                .appName("RedditPhotoImpact")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        JavaRDD<String> lines = jsc.textFile(InputPath);

        JavaPairRDD<Integer, Integer> impactPairs = lines
                .flatMap(line -> Arrays.asList(line.split("\n")).iterator())
                .mapToPair(line -> {
                    String[] columns = line.split(",");
                    int image_id = Integer.parseInt(columns[0].trim());
                    int upvotes = Integer.parseInt(columns[4].trim());
                    int downvotes = Integer.parseInt(columns[5].trim());
                    int comments = Integer.parseInt(columns[6].trim());
                    int impact = upvotes + downvotes + comments;
                    return new Tuple2<>(image_id, impact);
                });

        JavaPairRDD<Integer, Integer> imageImpact = impactPairs.reduceByKey((x, y) -> x + y);

        imageImpact.foreach(result -> System.out.println(result._1 + " " + result._2));

        spark.stop();
    }
}
