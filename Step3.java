import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import software.amazon.awssdk.services.s3.endpoints.internal.Value;
//import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

//import javax.xml.soap.Text;
import java.io.IOException;
import java.util.HashMap;


public class Step3 {
    /**
     * Input:
     * 1. N_r^0 And N_r^1
     * key - <'N',r, part>  (r=occurrences)
     * value - <1>
     * <p>
     * 2. T_r^0 AND T_r^1
     * key - <'T', r, part>  (r = total occurrences of the specific trigram in this part (0) of the corpus)
     * value - <total occurrences of the specific trigram in the other part (1) of the corpus>
     * <p>
     * 3. Total occurrences for each trigram
     * key: <w1, w2, w3>
     * value : <total occurrences>
     * Output:
     * 1. N or T
     * key - <r,'a'>  (r=occurrences)
     * value - <'N' OR 'T', part, occurrences>
     * 2. trigram
     * key - <r,'b'>  (r=occurrences)
     * value - <w1, w2, w3>
     */

    private static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] keyValuePair = value.toString().split("\t");
            String[] parsedKeys = keyValuePair[0].split(",");
            if (parsedKeys.length == 3) { // either N or T
                String NT = parsedKeys[0];
                String r = parsedKeys[1];
                String part = parsedKeys[2];
                String parsedValue = keyValuePair[1];
                context.write(new Text(r + ",a"), new Text(NT + "," + part + "," + parsedValue)); // <<r,a>, <N/T,part,parsedValue>>
            } else if (parsedKeys.length == 1) { // trigram
                context.write(new Text(keyValuePair[1] + ",b"), new Text(keyValuePair[0])); //<<r,b>, trigram>
            }
        }
    }

    /**
     * Input:
     * 1. N or T
     * key - <r,'a'>  (r=occurrences)
     * value - <'N' OR 'T', part, occurrences>
     * 2. trigram
     * key - <r,'b'>  (r=occurrences)
     * value - <w1, w2, w3>
     * Output:
     * trigram
     * key - <w1, w2, w3>
     * value - <probability>
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        static double N = 23260642968D;
        static double N01 = 0;
        static double T01 = 0;


        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] parsedKey = key.toString().split(",");
            if (parsedKey[1].equals("a")) {
                N01 = 0;
                T01 = 0;

                while (values.iterator().hasNext()) {
                    String value = values.iterator().next().toString();
                    String[] parsedValue = value.split(",");
                    if (parsedValue[0].equals("N")) {
                        N01 += Integer.valueOf(parsedValue[2]);
                    }
                    if (parsedValue[0].equals("T")) {
                        T01 += Integer.valueOf(parsedValue[2]);
                    }
                }
            } else if (parsedKey[1].equals("b")) {
                double probability;

                if (N01 == 0) {
                    probability = 0;
                } else {
                    probability = T01 / (N * N01);
                }
                String probabilityS = String.valueOf(probability);

                while (values.iterator().hasNext()) {
                    String trigram = values.iterator().next().toString();
                    context.write(new Text(trigram), new Text(probabilityS)); //
                }
            }
        }
    }


    public static class Partition extends Partitioner<Text, Text> {

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
        public int compare(WritableComparable key1, WritableComparable key2) {

            // key: r,a OR r,b
            String[] key11 = key1.toString().split(",");
            String[] key22 = key2.toString().split(",");
            int number1 = Integer.parseInt(key11[0]);
            int number2 = Integer.parseInt(key22[0]);
            if (number1 > number2) {
                return 1;
            } else if (number1 < number2) {
                return -1;
            } else {
                if (key11[1].equals(key22[1])) {
                    return 0;
                } else if (key11[1].equals("a")) {
                    return -1;
                } else {
                    return 1;
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step3.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Step3.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(Comparison.class);
        job.setPartitionerClass(Step3.Partition.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);

        boolean local = false;
        String input = "", output = "";
        if (local) {
            input = "/home/adler/Downloads/dist2-20221224T103516Z-001-20221228T114702Z-001/dist2-20221224T103516Z-001/dist2/MapReduceProject/src/main/java/outputs/Step2/part-r-00000";
            output = "/home/adler/Downloads/dist2-20221224T103516Z-001-20221228T114702Z-001/dist2-20221224T103516Z-001/dist2/MapReduceProject/src/main/java/outputs/Step3";
        } else {
            input = "s3://bucketurevich2/Step2output.txt";
            output = "s3://bucketurevich2/Step3output.txt";
        }

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}