package com.spark.streaming;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.ConfigurationBuilder;

public class SparkStreamingTwitter {

	public static void main(String[] args) throws InterruptedException {

		ConfigurationBuilder builder = new ConfigurationBuilder();

		builder.setOAuthConsumerKey("V06F9Q7VOg3KkrWudC06iLKCb");
		builder.setOAuthConsumerSecret("WQ3NG62eYxvOyG4l9S6kv0js1sM3WDEuNyFBR7HanJFN9r2ESt");
		builder.setOAuthAccessToken("86460677-a7iVXlaa4Bcly3PXbl3AdtyCKV8XJLclHdFDNylLl");
		builder.setOAuthAccessTokenSecret("Avw6TN4uHpTurENHojwgLBtaqBtYN79qjZhwaj557R6ma");

		TwitterFactory tf = new TwitterFactory(builder.build());
		Twitter twitter = tf.getInstance();

		Authorization twitterAuth = AuthorizationFactory.getInstance(twitter.getConfiguration());

		SparkConf sparkConf = new SparkConf().setAppName("Tweets-Analysis").setMaster("local[*]");
		JavaStreamingContext sc = new JavaStreamingContext(sparkConf, new Duration(1000));

		JavaReceiverInputDStream<Status> tweets = TwitterUtils.createStream(sc, twitterAuth);

		JavaDStream<String> words = tweets.flatMap(new FlatMapFunction<Status, String>() {
			public Iterable<String> call(Status s) {
				return Arrays.asList(s.getText().split(" "));
			}
		});

		JavaDStream<String> hashTags = words.filter(new Function<String, Boolean>() {
			public Boolean call(String word) {
				return word.startsWith("#");
			}
		});
		hashTags.print(10);
		sc.start();
		sc.awaitTermination();

	}

}
