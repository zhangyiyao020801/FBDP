package fis;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FreqItemSet {

    static class SubSet implements WritableComparable<SubSet> {
        private String subset;
        private Integer cnt;

        public SubSet() {
        }

        public SubSet(String subset, Integer cnt) {
            this.subset = subset;
            this.cnt = cnt;
        }

        public String getSubset() {
            return this.subset;
        }

        public void setSubset(String subset) {
            this.subset = subset;
        }

        public Integer getCnt() {
            return this.cnt;
        }

        public void setCnt(Integer cnt) {
            this.cnt = cnt;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.subset = in.readUTF();
            this.cnt = in.readInt();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(this.subset);
            out.writeInt(this.cnt);
        }

        @Override
        public int compareTo(SubSet s) {
            if (!this.cnt.equals(s.getCnt())) {
                return -this.cnt.compareTo(s.getCnt());
            } else {
                return this.subset.compareTo(s.getSubset());
            }
        }
    }

    public static class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int k = conf.getInt("k", 2);

            String[] cols = value.toString().split(" ");
            HashSet<String> hs = new HashSet<String>();
            for (int i = 0; i < cols.length; ++i) {
                hs.add(cols[i]);
            }
            List<String> items = new ArrayList<String>();
            for (String s : hs) {
                items.add(s);
            }
            if (items.size() >= k) {
                Collections.sort(items);
                Combination comb = new Combination(items, k);
                comb.execute();
                List<String> res = comb.getAns();
                for (String s : res) {
                    context.write(new Text(s), one);
                }
            }
        }
    }

    public static class Reducer1 extends Reducer<Text, IntWritable, Text, NullWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int min = conf.getInt("min", 2);
            int sum = 0;
            String s = key.toString();
            for (IntWritable val : values) {
                sum += val.get();
            }
            if (sum >= min) {
                context.write(new Text("(" + s + "):" + Integer.toString(sum)), NullWritable.get());
            }
        }
    }

    public static class Mapper2 extends Mapper<LongWritable, Text, SubSet, NullWritable> {

        private SubSet s = new SubSet();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] cols = value.toString().split(":");
            this.s.setSubset(cols[0]);
            this.s.setCnt(Integer.parseInt(cols[1]));
            context.write(s, NullWritable.get());
        }
    }

    public static class Reducer2 extends Reducer<SubSet, NullWritable, Text, NullWritable> {

        @Override
        public void reduce(SubSet key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(new Text(key.getSubset() + ", " + Integer.toString(key.getCnt())), NullWritable.get());
        }
    }

    public static class Partitioner2 extends Partitioner<SubSet, NullWritable> {
        @Override
        public int getPartition(SubSet key, NullWritable value, int numPartitions) {
            return Integer.parseInt(key.getSubset());
        }
    }

    public static class Comparator2 extends WritableComparator {
        protected Comparator2() {
            super(SubSet.class, true);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public int compare(WritableComparable w1, WritableComparable w2) {
            SubSet f1 = (SubSet) w1;
            SubSet f2 = (SubSet) w2;
            if (!f1.getCnt().equals(f2.getCnt())) {
                return -f1.getCnt().compareTo(f2.getCnt());
            } else {
                return f1.getSubset().compareTo(f2.getSubset());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        List<String> otherArgs = new ArrayList<String>();
        for (int i = 0; i < remainingArgs.length; ++i) {
            otherArgs.add(remainingArgs[i]);
        }

        conf.set("k", otherArgs.get(2));
        conf.set("min", otherArgs.get(3));

        Job job1 = Job.getInstance(conf);
        job1.setJarByClass(FreqItemSet.class);
        job1.setMapperClass(Mapper1.class);
        job1.setReducerClass(Reducer1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job1, new Path(otherArgs.get(0)));
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs.get(1) + "/step1"));
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf);
        job2.setJarByClass(FreqItemSet.class);
        job2.setMapperClass(Mapper2.class);
        job2.setReducerClass(Reducer2.class);
        job2.setMapOutputKeyClass(SubSet.class);
        job2.setMapOutputValueClass(NullWritable.class);
        job2.setPartitionerClass(Partitioner2.class);
        job2.setGroupingComparatorClass(Comparator2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job2, new Path(otherArgs.get(1) + "/step1"));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs.get(1) + "/final"));
        job2.waitForCompletion(true);
    }
}
