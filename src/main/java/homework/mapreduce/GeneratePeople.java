package homework.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Random;
import java.util.StringJoiner;

public class GeneratePeople extends Configured implements Tool {
    static class GenerateNamesMapper extends Mapper<Text, Text, Text, Text> {
        private int size;

        @Override
        protected void setup(Context context) {
            size = context.getConfiguration().getInt(SIZE, 10);
        }

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            for (int i = 0; i < size; i++) {
                int lastNameIndex = new Random().nextInt(LAST_NAMES.length);
                int firstNameIndex = new Random().nextInt(FIRST_NAMES.length);
                int locationIndex = new Random().nextInt(LOCATIONS.length);
                int birthDate = new Random().nextInt(5) + 1980;

                StringJoiner resultJoiner = new StringJoiner(",");
                resultJoiner.add(FIRST_NAMES[firstNameIndex]);
                resultJoiner.add(LAST_NAMES[lastNameIndex]);
                resultJoiner.add(LOCATIONS[locationIndex]);
                resultJoiner.add(Integer.toString(birthDate));

                Text result = new Text();
                result.set(resultJoiner.toString());
                context.write(result, new Text());
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);

        // No reduce
        job.setNumReduceTasks(0);
        // 1 mapper
        conf.setInt(MRJobConfig.NUM_MAPS, 1);

        job.setJarByClass(GeneratePeople.class);
        job.setJobName("generate-names");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(NoFileInputFormat.class);
        job.setMapperClass(GenerateNamesMapper.class);

        Class<? extends OutputFormat> outputFormatClass =
                TextOutputFormat.class;

        job.setOutputFormatClass(outputFormatClass);
        FileOutputFormat.setOutputPath(job, new Path(args[0]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new GeneratePeople(), args);
        System.exit(res);
    }

    // Stuff needed for generating names
    private static final String[] LAST_NAMES = {
            "Pollard"
            , "Rose"
            , "Frazier"
            , "Rubio"
    };

    private static final String[] FIRST_NAMES = {
            "Jaylene"
            , "Elizabeth"
            , "Laura"
            , "Jada"
            , "Kyra"
    };

    private static final String[] LOCATIONS = {
            "New York"
            , "San Francisco"
    };

    // The number of rows to generate in the CSV comes as a property.
    private static final String SIZE = "mapreduce.generatenames.size";

}
