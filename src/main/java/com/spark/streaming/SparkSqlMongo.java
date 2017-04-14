package com.spark.streaming;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.mongodb.spark.MongoSpark;

public class SparkSqlMongo {

	

	public static void main(String[] args) {

		SparkSession sparkSession = SparkSession.builder().master("local").appName("Spark-MongoDB")
				.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/school.students")
				.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/school.students")
				.config("spark.sql.warehouse.dir", "file:///C:/Users/Home/workspace/spark-mongodb/spark-warehouse")
				.getOrCreate();

		JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());

		Dataset<Row> load = MongoSpark.load(sparkContext).toDF();
		load.printSchema();

		Dataset<Characters> external = MongoSpark.load(sparkContext).toDS(Characters.class);
		external.printSchema();

		sparkContext.close();
	}

}
