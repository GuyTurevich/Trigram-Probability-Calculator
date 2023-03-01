import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

// args = s3 path of the output of step1
public class Step2 {
    /**
     * Input:
     *      Key: <w1, w2, w3>
     *      Value: [Total occurrences of all years in part 0, Total occurrences of all years in part 1]
     *
     * Output:
     *      1. N_r^0 And N_r^1
     *          key - <'N',r, part>  (r=occurrences)
     *          value - <1>
     *
     *      2. T_r^0 AND T_r^1
     *          key - <'T', r, part>  (r = total occurrences of the specific trigram in this part (0) of the corpus)
     *          value - <total occurrences of the specific trigram in the other part (1) of the corpus>
     *
     *      3. Total occurrences for each trigram
     *          key: <w1, w2, w3>
     *          value : <total occurrences>
     */
    private static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map (LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
            String[] keyValuePair = value.toString().split("\t");
            String trigram = keyValuePair[0];
            String[] occPerPart = keyValuePair[1].split(",");
            String occPart0 = occPerPart[0];
            String occPart1 = occPerPart[1];

            context.write(new Text("N," + occPart0 + ",0"), new Text("1")); // <<occurrences,part>, <1>>
            context.write(new Text("N," + occPart1 + ",1"), new Text("1")); // <<occurrences,part>, <1>>

            context.write(new Text("T," + occPart0 + ",0"), new Text(occPart1)); // <<T, r, 0>, <total occ...>>
            context.write(new Text("T," + occPart1 + ",1"), new Text(occPart0)); // <<T, r, 0>, <total occ...>>

            String r = "" + (Integer.valueOf(occPart0) + Integer.valueOf(occPart1));
            context.write(new Text(trigram) , new Text(r)); // <<w1,w2,w3>, total occ in all the corpus>

        }
    }
    

    /**
     * Sums up all T and N values for each r
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            while (values.iterator().hasNext()) {
                try {
                    sum += Integer.parseInt(values.iterator().next().toString());
                } catch (Exception ignored) {
                }
            }
            context.write(key, new Text(String.format("%d", sum)));
        }
    }

    public static class Partition extends Partitioner<Text,Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return Math.abs(key.hashCode() % numPartitions);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //Job job = new Job(conf);
        job.setJarByClass(Step2.class);
        job.setMapperClass(Map.class);
        //job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(Step2.Partition.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);

        boolean local = false;
        String input = "", output = "";
        if(local){
            input = "/home/adler/Downloads/dist2-20221224T103516Z-001-20221228T114702Z-001/dist2-20221224T103516Z-001/dist2/MapReduceProject/src/main/java/outputs/Step1/part-r-00000"; //"s3://bucketurevich2/output1.txt/";
            output = "/home/adler/Downloads/dist2-20221224T103516Z-001-20221228T114702Z-001/dist2-20221224T103516Z-001/dist2/MapReduceProject/src/main/java/outputs/Step2"; //"s3://bucketurevich2/output2"; //"s3://input-file-hadoop/Step1.jar" path in s3 for the output
        }
        else{
            input = "s3://bucketurevich2/Step1output.txt";
            output = "s3://bucketurevich2/Step2output.txt";
        }

        FileInputFormat.addInputPath(job, new Path(input));
        TextOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
