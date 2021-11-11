package org.apache.spark.examples.sql.streaming;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Arrays;
import java.util.Iterator;

public class JavaWordCount {
	
	public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println("Please provide the full path of input file and output dir as arguments");
      System.exit(0);
    }

    
    SparkSession spark = SparkSession
    		  .builder()
    		  .master("local")
    		  .appName("WordCount")
    		  .getOrCreate();
    
    
    Dataset<String> df = spark.read().text(args[0]).as(Encoders.STRING());
    Dataset<String> words = df.flatMap(new FlatMapFunction<String, String>() {
              				public Iterator<String> call(String x) {
                				return Arrays.asList(x.split(" ")).iterator();
              					}	
            				},
            				Encoders.STRING());
    
    Dataset<Row> t = words.groupBy("value") //<k, iter(V)>
    		.count()
    		.toDF("word","count");
    t = t.sort(functions.desc("count"));
    t.toJavaRDD().saveAsTextFile(args[1]);
  }
}
