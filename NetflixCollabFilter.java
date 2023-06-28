package com.RUSpark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

public class NetflixCollabFilter {

  public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: NetflixCollabFilter <file>");
      System.exit(1);
    }

    String inputPath = args[0];

    Map<Integer, Map<Integer, Double>> userRatings = readData(inputPath);

    Map<Integer, List<Integer>> recommendedMovies = getRecommendations(userRatings, 11, 3);

    for (Map.Entry<Integer, List<Integer>> entry : recommendedMovies.entrySet()) {
      System.out.println(entry.getKey() + ", " + entry.getValue().toString());
    }
  }

  public static Map<Integer, Map<Integer, Double>> readData(String inputPath) throws IOException {
    Map<Integer, Map<Integer, Double>> userRatings = new HashMap<>();

    BufferedReader br = new BufferedReader(new FileReader(inputPath));
    String line;
    while ((line = br.readLine()) != null) {
      String[] parts = line.split(",");
      int userId = Integer.parseInt(parts[0]);
      int movieId = Integer.parseInt(parts[1]);
      double rating = Double.parseDouble(parts[2]);

      userRatings.putIfAbsent(userId, new HashMap<>());
      userRatings.get(userId).put(movieId, rating);
    }
    br.close();

    return userRatings;
  }

  public static double cosineSimilarity(Map<Integer, Double> ratings1, Map<Integer, Double> ratings2) {
    double dotProduct = 0.0;
    double norm1 = 0.0;
    double norm2 = 0.0;

    for (int movieId : ratings1.keySet()) {
      double rating1 = ratings1.get(movieId);
      norm1 += rating1 * rating1;
      if (ratings2.containsKey(movieId)) {
        double rating2 = ratings2.get(movieId);
        dotProduct += rating1 * rating2;
      }
    }

    for (double rating : ratings2.values()) {
      norm2 += rating * rating;
    }

    if (norm1 == 0 || norm2 == 0) {
      return 0;
    }

    return dotProduct / (Math.sqrt(norm1) * Math.sqrt(norm2));
  }

  public static Map<Integer, List<Integer>> getRecommendations(Map<Integer, Map<Integer, Double>> userRatings, int k, int m) {
    Map<Integer, List<Integer>> recommendedMovies = new HashMap<>();

    for (int userId : userRatings.keySet()) {
      PriorityQueue<Map.Entry<Integer, Double>> topKNeighbors = new PriorityQueue<>(k, Comparator.comparingDouble(Map.Entry::getValue));
      for (int neighborId : userRatings.keySet()) {
        if (userId == neighborId) {
          continue;
        }

        double similarity = cosineSimilarity(userRatings.get(userId), userRatings.get(neighborId));
        topKNeighbors.add(new HashMap.SimpleEntry<>(neighborId, similarity));
                if (topKNeighbors.size() > k) {
          topKNeighbors.poll();
        }
      }

      Map<Integer, Double> predictedRatings = new HashMap<>();
      for (Map.Entry<Integer, Double> entry : topKNeighbors) {
        int neighborId = entry.getKey();
        double similarity = entry.getValue();
        Map<Integer, Double> neighborRatings = userRatings.get(neighborId);

        for (int movieId : neighborRatings.keySet()) {
          if (!userRatings.get(userId).containsKey(movieId)) {
            predictedRatings.put(movieId, predictedRatings.getOrDefault(movieId, 0.0) + similarity * neighborRatings.get(movieId));
          }
        }
      }

      List<Integer> topMMovies = predictedRatings.entrySet().stream()
          .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
          .limit(m)
          .map(Map.Entry::getKey)
          .collect(Collectors.toList());

      recommendedMovies.put(userId, topMMovies);
    }

    return recommendedMovies;
  }
}

