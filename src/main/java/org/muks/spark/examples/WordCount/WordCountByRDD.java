package org.muks.spark.examples.WordCount;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class WordCountByRDD {
    private final static String inputFilePath = "/Users/mukthara/Data/git/personal/ScalaProgramming/src/main/resources/word_count.txt";
    private final static String outputPath = "/Users/mukthara/Data/git/personal/ScalaProgramming/CountDataOutput";

    public static void main(String[] args) {

        deleteLocalOutput();    // local dir cleanup

        SparkConf sparkConf =
                new SparkConf()
                        .setMaster("local")
                        .setAppName("JD Word Counter");

        JavaRDD<String> inputFile;
        try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {

            inputFile = sparkContext.textFile(inputFilePath, 3);

            System.out.format("= Current partitions = %s", inputFile.partitions().size());

            JavaPairRDD<String, Integer> countData
                    = inputFile
                    .flatMap(
                            (s) -> Arrays.asList(s.split(" ")).iterator()
                    )
                    .mapToPair(
                            word -> new Tuple2<>(word, 1)
                    )
                    .reduceByKey(
                            (a, b) -> a + b
                    );


            countData.saveAsTextFile(outputPath);

        } catch (Exception e) {
            e.printStackTrace();

        } finally {

        }

    }


    private static void deleteLocalOutput() {
        try {
            FileUtils.deleteDirectory(new File(outputPath));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
