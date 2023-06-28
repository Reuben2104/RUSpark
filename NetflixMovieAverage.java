package com.RUSpark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;

public class NetflixMovieAverage {

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: NetflixMovieAverage <file>");
            System.exit(1);
        }

        String InputPath = args[0];

        SparkSession spark = SparkSession
                .builder()
                .appName("NetflixMovieAverage")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        JavaRDD<String> lines = jsc.textFile(InputPath);

        JavaPairRDD<Integer, Tuple2<Double, Integer>> movieRatings = lines
                .flatMap(line -> Arrays.asList(line.split("\n")).iterator())
                .mapToPair(line -> {
                    String[] columns = line.split(",");
                    int movie_id = Integer.parseInt(columns[0].trim());
                    double rating = Double.parseDouble(columns[2].trim());
                    return new Tuple2<>(movie_id, new Tuple2<>(rating, 1));
                });

        JavaPairRDD<Integer, Tuple2<Double, Integer>> movieRatingSums = movieRatings.reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2));

        JavaPairRDD<Integer, Double> movieAverages = movieRatingSums.mapValues(x -> x._1 / x._2);

        movieAverages.foreach(result -> System.out.println(result._1 + " " + String.format("%.2f", result._2)));

        spark.stop();
    }
}
