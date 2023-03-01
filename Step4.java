import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


public class Step4 {
    /**
     * Map function reorganize the key and value -
     *     Input: same as the output of the reduce of Step3.
     *     Output:
     *          key - <w1, w2, probability>
     *          value - <w3>
     */
    private static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map (LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
            String[] keyValuePair = value.toString().split("\t");
            String[] words = keyValuePair[0].split(" ");
            String probability = keyValuePair[1];
            context.write(new Text(words[0] + " " + words[1] + " " + probability), new Text(words[2]));
        }
    }
 /**   Input: 
     *    key - <w1, w2, probability>
     *    value - <w3>
     * Output:
     *    key - <w1, w2, w3>
     *    value - <probability>
*/
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String[] parsedKey = key.toString().split(" ");
            String w1 = parsedKey[0];
            String w2 = parsedKey[1];
            String probability = parsedKey[2];
            if(probability.equals("0.0")) {
                return;
            }
            while (values.iterator().hasNext()) {
                String w3 = values.iterator().next().toString();
                context.write(new Text(w1 + " " + w2 + " " + w3), new Text(probability));
            }
        }
    }


    public static class Partition extends Partitioner<Text,Text>{

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return Math.abs(key.hashCode() % numPartitions);
        }
    }

    private static class Comparison extends WritableComparator {
        protected Comparison() {
            super(Text.class, true);
        }
        @Override
        /**Sorts the output of the map by the following rules:
                *    1) Left part of the key (w1): ascending
                *    2) Middle part of the key (w2): ascending
                *    3) Right of the key (probability): descending
                */
        public int compare(WritableComparable key1, WritableComparable key2) {
            String[] words1 = key1.toString().split(" ");
            String[] words2 = key2.toString().split(" ");

            for (int i = 0; i < 2; i++) {
                int compareResult = words1[i].compareTo(words2[i]);
                if (compareResult != 0) {
                    return compareResult;
                }
            }

            return words2[2].compareTo(words1[2]);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step4.class);
        job.setMapperClass(Step4.Map.class);
        job.setReducerClass(Step4.Reduce.class);
        job.setSortComparatorClass(Step4.Comparison.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(Step4.Partition.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);

        boolean local = false;
        String input = "", output = "";
        if(local){
            input = "/home/adler/Downloads/dist2-20221224T103516Z-001-20221228T114702Z-001/dist2-20221224T103516Z-001/dist2/MapReduceProject/src/main/java/outputs/Step3/part-r-00000";
            output = "/home/adler/Downloads/dist2-20221224T103516Z-001-20221228T114702Z-001/dist2-20221224T103516Z-001/dist2/MapReduceProject/src/main/java/outputs/Step4";
        }
        else{
            input = "s3://bucketurevich2/Step3output.txt";
            output = "s3://bucketurevich2/Step4output.txt";
        }

        FileInputFormat.addInputPath(job, new Path(input));
        TextOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}