package homework.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class CountTriplets {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CountTriplets <path> <table_name>");
            System.exit(1);
        }

        String inputPath = args[0];
        String tableName = args[1];

        SparkSession spark = SparkSession
                .builder()
                .appName("CountTriplets")
                .getOrCreate();

        // Read all part-* files.
        JavaRDD<String> lines = spark.read().textFile(ensureTrailingSlash(inputPath) + "part-*").javaRDD();
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

        try (HBaseLoader hBaseLoader = new HBaseLoader(tableName)) {
            hBaseLoader.put(people);
        }

        new PhoenixMappingTable(tableName).create();

        spark.stop();
    }

    private static String ensureTrailingSlash(String path){
        if(!path.endsWith("/")){
            return path + "/";
        }

        return path;
    }
}
