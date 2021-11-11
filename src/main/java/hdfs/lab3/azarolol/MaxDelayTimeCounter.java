package hdfs.lab3.azarolol;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class MaxDelayTime {
    final static String AppName = "MaxDelayTimeCounter";

    public static void mian (String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName(AppName);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airports = sc.textFile("L_AIRPORT_ID.csv");
        JavaRDD<String> flights = sc.textFile("")
    }
}