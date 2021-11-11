package hdfs.lab3.azarolol;

public class MaxDelayTime {
    final static String AppName = "MaxDelayTimeCounter"
    public static void mian (String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName(AppName);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airports = sc.textFile("L_AIRPORT_ID.csv")
    }
}