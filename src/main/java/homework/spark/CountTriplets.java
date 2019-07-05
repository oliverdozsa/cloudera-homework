package homework.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class CountTriplets {
    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: CountTriplets <file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("CountTriplets")
                .getOrCreate();


        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
        JavaPairRDD<String, Integer> ones = lines.mapToPair(s -> {
            String result = s.substring(0, s.lastIndexOf(";"));
            return new Tuple2<>(result, 1);
        });

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(Integer::sum);

        List<Tuple2<String, Integer>> output = counts.collect();
        List<LoadHBase.Person> people = new ArrayList<>();
        for (Tuple2<String, Integer> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
            people.add(new LoadHBase.Person(tuple._1(), tuple._2()));
        }

        try (LoadHBase hBaseLoader = new LoadHBase()) {
            hBaseLoader.put(people);
        }

        spark.stop();
    }
}
