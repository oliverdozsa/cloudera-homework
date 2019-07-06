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

        // Read all part-* files.
        JavaRDD<String> lines = spark.read().textFile(args[0] + "/part-*").javaRDD();
        JavaPairRDD<String, Integer> ones = lines.mapToPair(s -> {
            // Birthdate is the last part, but we don't need that.
            String result = s.substring(0, s.lastIndexOf(","));
            return new Tuple2<>(result, 1);
        });

        // Count by key == count by name and location
        JavaPairRDD<String, Integer> counts = ones.reduceByKey(Integer::sum);

        // Convert tuples to people
        List<Tuple2<String, Integer>> output = counts.collect();
        List<Person> people = new ArrayList<>();
        for (Tuple2<String, Integer> tuple : output) {
            people.add(new Person(tuple._1(), tuple._2()));
        }

        try (HBaseLoader hBaseLoader = new HBaseLoader()) {
            hBaseLoader.put(people);
        }

        PhoenixMappingTable.create();

        spark.stop();
    }
}
