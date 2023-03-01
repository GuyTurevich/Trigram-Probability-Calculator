import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import java.io.*;
import java.util.Collections;
import java.util.HashSet;


public class Step1 {

    static HashSet<String> stopWords = new HashSet<>();


    /**
     * Input:
     * key - lineId
     * value - 3-gram /t year /t occurrences /t pages /t books
     * <p>
     * Output:
     * Key: <w1, w2, w3>
     * Value: [<occurrences in part 0>,0] OR [0,<occurrences in part 1>]
     */
    private static class Map extends Mapper<LongWritable, Text, Text, Text> {
        int part = 0;

        @Override
        public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            //System.out.println("Started Mapping\n");
            String[] fields = value.toString().split("\t");
            String[] trigram = fields[0].split(" ");
            if (fields.length < 5)
                return;
            if (trigram.length > 2) {
                String w1 = trigram[0];
                String w2 = trigram[1];
                String w3 = trigram[2];

                if (isntLetter(w1) || isntLetter(w2) || isntLetter(w3))
                    return;

                if (isStopWord(w1) || isStopWord(w2) || isStopWord(w3))
                    return;

                String occurrences = fields[2];

                if (part == 0) {
                    part = 1;
                    context.write(new Text(String.format("%s %s %s", w1, w2, w3)), new Text(occurrences + ",0")); // for T
                } else {
                    part = 0;
                    context.write(new Text(String.format("%s %s %s", w1, w2, w3)), new Text("0," + occurrences)); // for T
                }

            }
        }

        private boolean isntLetter(String w1) {
            if (w1.isEmpty()) {
                return true;
            }
            for (int i = 0; i < w1.length(); i++) {
                if (!Character.isLetter(w1.charAt(i))) {
                    return true;
                }
            }
            return false;
        }

        private boolean isStopWord(String w1) {
            return stopWords.contains(w1);
        }
    }

    /**
     * Input:
     * Key: <w1, w2, w3>
     * Value: [<occurrences in part 0>,0] OR [0,<occurrences in part 1>]
     * <p>
     * Output:
     * Key: <w1, w2, w3>
     * Value: [<Total occurrences of all years in part 0>, <Total occurrences of all years in part 1>]
     */

        public static class Reduce extends Reducer<Text, Text, Text, Text> {
            @Override
            protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

                int part0 = 0, part1 = 0;

                while (values.iterator().hasNext()) {
                    String value = values.iterator().next().toString();
                    if (value.charAt(0) == '0') {
                        part1 += Integer.parseInt(value.substring(2));
                    } else {
                        part0 += Integer.parseInt(value.substring(0, value.indexOf(',')));
                    }
                }
                    String toWrite = part0 + "," + part1;
                    context.write( key, new Text(String.format("%s", toWrite)));
                }
            }



        public static class Partition extends Partitioner<Text, Text> {

            @Override
            public int getPartition(Text key, Text value, int numPartitions) {
                return Math.abs(key.hashCode() % numPartitions);
            }
        }

        public static void main(String[] args) throws Exception {

            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);
            job.setJarByClass(Step1.class);
            job.setMapperClass(Map.class);
            //job.setCombinerClass(Reduce.class);
            job.setReducerClass(Reduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setPartitionerClass(Step1.Partition.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setInputFormatClass(TextInputFormat.class);

            boolean local = false;
            boolean english = true;
            String input = "", output = "";
            if (local) {
                input = "/home/adler/Downloads/dist2-20221224T103516Z-001-20221228T114702Z-001/dist2-20221224T103516Z-001/dist2/MapReduceProject/3_grams.txt";
                output = "/home/adler/Downloads/dist2-20221224T103516Z-001-20221228T114702Z-001/dist2-20221224T103516Z-001/dist2/MapReduceProject/src/main/java/outputs/Step1";
                TextInputFormat.addInputPath(job, new Path(input));
                job.setInputFormatClass(TextInputFormat.class);
            } else {
                if (english) {
                    Collections.addAll(stopWords, "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll");
                    input = "s3n://datasets.elasticmapreduce/ngrams/books/20090715/eng-all/3gram/data";
                } else {
                    Collections.addAll(stopWords, "את", "לא", "של", "אני", "על", "זה", "עם", "כל", "הוא", "אם", "או", "גם", "יותר", "יש", "לי", "מה", "אבל", "פורום", "אז", "טוב", "רק", "כי", "שלי", "היה", "בפורום", "אין", "עוד", "היא", "אחד", "ב", "ל", "עד", "לך", "כמו", "להיות", "אתה", "כמה", "אנחנו", "הם", "כבר", "אנשים", "אפשר", "תודה", "שלא", "אותו", "ה", "מאוד", "הרבה", "ולא", "ממש", "לו", "א", "מי", "חיים", "בית", "שאני", "יכול", "שהוא", "כך");
                    input = "s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data";
                }
                output = "s3://bucketurevich2/Step1output.txt";
                SequenceFileInputFormat.addInputPath(job, new Path(input));
                job.setInputFormatClass(SequenceFileInputFormat.class);
            }

            FileOutputFormat.setOutputPath(job, new Path(output));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }


    }

