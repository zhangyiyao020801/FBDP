package wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

public class WordCount_by_year {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        static enum CountersEnum {
            INPUT_WORDS
        };

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Configuration conf;
        private boolean caseSensitive;
        private BufferedReader fis;
        private Set<String> patternsToSkip = new HashSet<String>();

        public void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            if (conf.getBoolean("wordcount.skip.patterns", false)) {
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
                for (URI patternsURI : patternsURIs) {
                    Path patternsPath = new Path(patternsURI.getPath());
                    String patternsFileName = patternsPath.getName().toString();
                    parseSkipFile(patternsFileName);
                }
            }
        }

        private void parseSkipFile(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) {
                    patternsToSkip.add(pattern);
                }

                // Single characters should be skipped additionally.
                for (int i = 0; i < 26; ++i) {
                    patternsToSkip.add(new String(new char[] { (char) (i + 'a') }));
                }

            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                        + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();

            // case sensitive
            caseSensitive = conf.getBoolean("wordcount.case.sensitive", false);
            String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();

            // check whether the format is legal (year + news)
            if (line.length() < 4) {
                return;
            }
            String year = line.substring(0, 4);

            // ignore punctuations
            line = " " + line + " ";
            line = line.replaceAll("\\pP", " ")
                    .replaceAll("[^a-zA-Z\\s]", " ");

            // ignore words to skip
            for (int i = 0; i < 5; ++i) {
                for (String pattern : patternsToSkip) {
                    line = line.replaceAll(" " + pattern + " ", " ");
                }
                line = line.replaceAll("[\\s+]", " ");
            }

            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                String tmp = word.toString();
                Text ans = new Text(year + tmp);
                context.write(ans, one);
                Counter counter = context.getCounter(CountersEnum.class.getName(),
                        CountersEnum.INPUT_WORDS.toString());
                counter.increment(1);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class SortMapper extends Mapper<Object, Text, IntWritable, Text> {
        private final static IntWritable wordCount = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void map(Object key, Text value,
                Mapper<Object, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreTokens()) {
                String a = tokenizer.nextToken().trim();
                word.set(a);
                String b = tokenizer.nextToken().trim();
                wordCount.set(Integer.parseInt(b));
                context.write(wordCount, word);
            }
        }
    }

    public static class SortPartitioner<K, V> extends Partitioner<K, V> {
        @Override
        public int getPartition(K key, V value, int numReduceTasks) {
            int maxValue = 50;
            int keySection = 0;
            if (numReduceTasks > 1 && key.hashCode() < maxValue) {
                int sectionValue = maxValue / (numReduceTasks - 1);
                int count = 0;
                while ((key.hashCode() - sectionValue * count) > sectionValue) {
                    count++;
                }
                keySection = numReduceTasks - 1 - count;
            }
            return keySection;
        }
    }

    public static class TestPartitioner<K, V> extends Partitioner<K, V> {
        @Override
        public int getPartition(K key, V value, int numReduceTasks) {
            String date = value.toString();
            String judge = date.substring(0, 4);
            return Integer.parseInt(judge) - 2008;
        }
    }

    public static class SortReducer extends Reducer<IntWritable, Text, Text, NullWritable> {
        private Text result = new Text();
        int rank = 1;

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values,
                Reducer<IntWritable, Text, Text, NullWritable>.Context context)
                throws IOException, InterruptedException {
            for (Text val : values) {
                if (rank > 100) {
                    break;
                }
                result.set(val.toString().substring(4));
                String str = rank + ": " + result + ", " + key;
                ++rank;
                context.write(new Text(str), NullWritable.get());
            }
        }
    }

    public static class DescWritableComparator extends IntWritable.Comparator {
        @Override
        @SuppressWarnings("rawtypes")
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        GenericOptionsParser optionsParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionsParser.getRemainingArgs();

        if (!(remainingArgs.length != 2 || remainingArgs.length != 4)) {
            System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
            System.exit(2);
        }

        // wordcount by year
        Job job1 = Job.getInstance(conf, "word count by year");
        job1.setJarByClass(WordCount_by_year.class);
        job1.setMapperClass(TokenizerMapper.class);
        job1.setCombinerClass(IntSumReducer.class);
        job1.setReducerClass(IntSumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        List<String> otherArgs = new ArrayList<String>();
        for (int i = 0; i < remainingArgs.length; ++i) {
            if ("-skip".equals(remainingArgs[i])) {
                job1.addCacheFile(new Path(remainingArgs[++i]).toUri());
                job1.getConfiguration().setBoolean("wordcount.skip.patterns", true);
            } else {
                otherArgs.add(remainingArgs[i]);
            }
        }
        FileInputFormat.addInputPath(job1, new Path(otherArgs.get(0)));
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs.get(1)));
        job1.waitForCompletion(true);

        // wordsort by year
        Job job2 = Job.getInstance(conf, "word sort by year");
        job2.setJarByClass(WordCount_by_year.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        job2.setMapperClass(WordCount_by_year.SortMapper.class);
        job2.setReducerClass(WordCount_by_year.SortReducer.class);
        job2.setSortComparatorClass(DescWritableComparator.class);
        job2.setNumReduceTasks(9);
        job2.setPartitionerClass(WordCount_by_year.TestPartitioner.class);
        FileInputFormat.addInputPath(job2, new Path(otherArgs.get(1)));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs.get(2)));
        job2.waitForCompletion(true);

        System.out.println("Job Finish!");
        System.exit(1);
    }
}
