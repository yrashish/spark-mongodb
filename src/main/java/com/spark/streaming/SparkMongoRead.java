package com.spark.streaming;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

public class SparkMongoRead {

	public static void main(String[] args) throws InterruptedException {
		SparkSession sparkSession = SparkSession.builder()
				.master("local")
				.appName("Spark-MongoDB")
				.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/school.students")
				.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/school.students")
				.getOrCreate();
		
		JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());
		
		JavaMongoRDD<Document> mongoRDD = MongoSpark.load(sparkContext);
		
		System.out.println("Total number of records " + mongoRDD.count());
		
		System.out.println("First record " + mongoRDD.first().toJson());
		
		sparkContext.close();
	}

}
