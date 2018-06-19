//package org.muks.spark.examples;
//
//import org.apache.spark.sql.*
//
//import java.util.Arrays;
//
//public class WordCountByDataset {
//    public static void main(String[] args) {
//        String inputFilePath = "/Users/mukthara/Data/git/personal/SparkProgramming/src/main/resources/word_count.txt";
//
//        SparkSession spark = SparkSession
//                .builder()
//                .master("local")
//                .appName("WordCount")
//                .getOrCreate();
//
//
//        Dataset<String> df = spark.read().text(args[0]).as(Encoders.STRING());
//        Dataset<String> words = df.flatMap(s -> {
//            return  Arrays.asList(s.toLowerCase().split(" ")).iterator();
//        }, Encoders.STRING())
//                .filter(s -> !s.isEmpty())
//                .coalesce(1); //one partition (parallelism level)
//        //words.printSchema();   // { value: string (nullable = true) }
//        Dataset<Row> t = words.groupBy("value") //<k, iter(V)>
//                .count()
//                .toDF("word","count");
//        t = t.sort(functions.desc("count"));
//        t.toJavaRDD().saveAsTextFile(args[1]);
//
//
//    }
//}
