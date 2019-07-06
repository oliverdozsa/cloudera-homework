package homework.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// Based on: https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-examples/src/main/java/org/apache/hadoop/examples/RandomWriter.java
class NoFileInputFormat extends InputFormat<Text, Text> {

    /**
     * Generate the requested number of file splits, with the filename
     * set to the filename of the output file.
     */
    public List<InputSplit> getSplits(JobContext job) {
        List<InputSplit> result = new ArrayList<>();
        Path outDir = FileOutputFormat.getOutputPath(job);

        // One split
        int numSplits = 1;
        for (int i = 0; i < numSplits; ++i) {
            result.add(new FileSplit(new Path(outDir, "dummy-split-" + i), 0, 1,
                    null));
        }

        return result;
    }

    /**
     * Return a single record (filename, "") where the filename is taken from
     * the file split.
     */
    static class RandomRecordReader extends RecordReader<Text, Text> {
        Path name;
        Text key = null;
        Text value = new Text();

        public RandomRecordReader(Path p) {
            name = p;
        }

        public void initialize(InputSplit split,
                               TaskAttemptContext context) {

        }

        public boolean nextKeyValue() {
            if (name != null) {
                key = new Text();
                key.set(name.getName());
                name = null;
                return true;
            }
            return false;
        }

        public Text getCurrentKey() {
            return key;
        }

        public Text getCurrentValue() {
            return value;
        }

        public void close() {
        }

        public float getProgress() {
            return 0.0f;
        }
    }

    public RecordReader<Text, Text> createRecordReader(InputSplit split,
                                                       TaskAttemptContext context) throws IOException, InterruptedException {
        return new RandomRecordReader(((FileSplit) split).getPath());
    }
}
