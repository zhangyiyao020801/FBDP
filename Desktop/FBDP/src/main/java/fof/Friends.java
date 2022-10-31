package fof;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class Friends {

    static class FriendsPair implements WritableComparable<FriendsPair> {
        private String friend;
        private String union;

        public String getFriend() {
            return friend;
        }

        public void setFriend(String friend) {
            this.friend = friend;
        }

        public String getUnion() {
            return union;
        }

        public void setUnion(String union) {
            this.union = union;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(this.friend);
            out.writeUTF(this.union);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.friend = in.readUTF();
            this.union = in.readUTF();
        }

        @Override
        public int compareTo(FriendsPair o) {
            if (this.friend.equals(o.getFriend())) {
                return this.union.compareTo(o.getUnion());
            } else {
                return this.friend.compareTo(o.getFriend());
            }
        }
    }

    static class NewFriend implements WritableComparable<NewFriend> {
        private String person;
        private String friend;
        private Integer cnt;
        private String info;

        public NewFriend() {
        }

        public NewFriend(String person, String friend, Integer cnt, String info) {
            this.person = person;
            this.friend = friend;
            this.cnt = cnt;
            this.info = info;
        }

        public String getPerson() {
            return person;
        }

        public void setPerson(String person) {
            this.person = person;
        }

        public String getFriend() {
            return friend;
        }

        public void setFriend(String friend) {
            this.friend = friend;
        }

        public Integer getCnt() {
            return cnt;
        }

        public void setCnt(Integer cnt) {
            this.cnt = cnt;
        }

        public String getInfo() {
            return info;
        }

        public void setInfo(String info) {
            this.info = info;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.person = in.readUTF();
            this.friend = in.readUTF();
            this.cnt = in.readInt();
            this.info = in.readUTF();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(this.person);
            out.writeUTF(this.friend);
            out.writeInt(this.cnt);
            out.writeUTF(this.info);
        }

        @Override
        public int compareTo(NewFriend o) {
            if (!this.cnt.equals(o.getCnt())) {
                return this.cnt.compareTo(o.getCnt());
            } else if (!this.friend.equals(o.getFriend())) {
                return -(Integer.parseInt(this.friend) - Integer.parseInt(o.getFriend()));
            } else {
                return -this.person.compareTo(o.getPerson());
            }
        }
    }

    public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] cols = value.toString().split("\t");
            String person1 = cols[0], person2 = cols[1];
            context.write(new Text(person1), new Text(person2));
        }
    }

    public static class Reducer1 extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer stringbuffer = new StringBuffer();
            for (Text text : values) {
                stringbuffer.append(text).append(",");
            }
            String str = stringbuffer.toString();
            str = str.trim();
            if (str.length() > 0) {
                str = str.substring(0, str.length() - 1);
            }
            str = new StringBuffer(str).reverse().toString();
            context.write(key, new Text(str));
        }
    }

    public static class Mapper2 extends Mapper<LongWritable, Text, Text, FriendsPair> {
        private Text map_key = new Text();
        private FriendsPair map_pair = new FriendsPair();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] col = value.toString().split("\t");
            this.map_key.set(col[0]);
            String[] friends = col[1].split(",");
            for (int i = 0; i < friends.length; ++i) {
                this.map_pair.setFriend(friends[i]);
                this.map_pair.setUnion("yes");
                context.write(this.map_key, this.map_pair);
            }
            for (int i = 0; i < friends.length; ++i) {
                for (int j = i + 1; j < friends.length; ++j) {
                    this.map_key.set(friends[i]);
                    this.map_pair.setFriend(friends[j]);
                    this.map_pair.setUnion(col[0]);
                    context.write(this.map_key, this.map_pair);

                    this.map_key.set(friends[j]);
                    this.map_pair.setFriend(friends[i]);
                    this.map_pair.setUnion(col[0]);
                    context.write(this.map_key, this.map_pair);
                }
            }
        }
    }

    public static class Reducer2 extends Reducer<Text, FriendsPair, Text, Text> {

        public void reduce(Text key, Iterable<FriendsPair> values, Context context)
                throws IOException, InterruptedException {
            Multimap<String, String> mp = HashMultimap.create();
            for (FriendsPair x : values) {
                mp.put(x.getFriend(), x.getUnion());
            }
            Set<String> st = mp.keySet();
            boolean has_recommended = false;
            for (String x : st) {
                boolean is_ok = true;
                Collection<String> collection = mp.get(x);
                String tmp = new String();
                for (String y : collection) {
                    if (y.equals("yes")) {
                        is_ok = false;
                        break;
                    }
                    tmp += y + ",";
                }
                if (tmp.length() > 1) {
                    tmp = tmp.substring(0, tmp.length() - 1);
                }
                if (is_ok) {
                    has_recommended = true;
                    String s = x + ":[" + tmp.trim() + "]";
                    context.write(new Text(key), new Text(collection.size() + ":" + s));
                }
            }
            if (!has_recommended) {
                context.write(new Text(key), new Text("0"));
            }
        }
    }

    public static class Mapper3 extends Mapper<LongWritable, Text, NewFriend, NullWritable> {

        private NewFriend map_key = new NewFriend();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] cols = value.toString().split("\t");
            this.map_key.setPerson(cols[0]);
            String[] s = cols[1].split(":");
            if (s.length == 1) {
                this.map_key.setFriend("");
                this.map_key.setCnt(0);
                this.map_key.setInfo("");
            } else {
                this.map_key.setFriend(s[1]);
                this.map_key.setCnt(Integer.parseInt(s[0]));
                this.map_key.setInfo(s[2]);
            }
            context.write(this.map_key, NullWritable.get());
        }
    }

    public static class Partitioner3 extends Partitioner<NewFriend, NullWritable> {
        @Override
        public int getPartition(NewFriend key, NullWritable value, int numPartitions) {
            return Integer.parseInt(key.getPerson());
        }
    }

    public static class Comparator3 extends WritableComparator {
        protected Comparator3() {
            super(NewFriend.class, true);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public int compare(WritableComparable w1, WritableComparable w2) {
            NewFriend f1 = (NewFriend) w1;
            NewFriend f2 = (NewFriend) w2;
            if (!f1.getCnt().equals(f2.getCnt())) {
                return f1.getCnt().compareTo(f2.getCnt());
            } else if (!f1.getFriend().equals(f2.getFriend())) {
                return -(Integer.parseInt(f1.getFriend()) - Integer.parseInt(f2.getFriend()));
            } else {
                return -f1.getPerson().compareTo(f2.getPerson());
            }
        }
    }

    public static class Reducer3 extends Reducer<NewFriend, NullWritable, Text, Text> {
        public void reduce(NewFriend key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            String person = key.getPerson();
            String friend = key.getFriend();
            String info = key.getInfo();
            Integer cnt = key.getCnt();
            if (cnt == 0) {
                context.write(new Text(person), new Text(""));
            } else {
                context.write(new Text(person), new Text(friend + "(" + cnt.toString() + ":" + info + ")"));
            }
        }
    }

    public static class Mapper4 extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] cols = value.toString().split("\t");
            if (cols.length < 2) {
                context.write(new Text(cols[0]), new Text(""));
            } else {
                context.write(new Text(cols[0]), new Text(cols[1]));
            }
        }
    }

    public static class Reducer4 extends Reducer<Text, Text, Text, NullWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            StringBuffer stringbuffer = new StringBuffer();
            for (Text text : values) {
                stringbuffer.append(text).append(", ");
            }
            String str = stringbuffer.toString();
            if (str.length() >= 2) {
                str = str.substring(0, str.length() - 2);
            }
            context.write(new Text(key + ": " + str), NullWritable.get());
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

        Job job1 = Job.getInstance(conf);
        job1.setJarByClass(Friends.class);
        job1.setMapperClass(Mapper1.class);
        job1.setReducerClass(Reducer1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job1, new Path(otherArgs.get(0)));
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs.get(1) + "/step1"));
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf);
        job2.setJarByClass(Friends.class);
        job2.setMapperClass(Mapper2.class);
        job2.setReducerClass(Reducer2.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(FriendsPair.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job2, new Path(otherArgs.get(1) + "/step1"));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs.get(1) + "/step2"));
        job2.waitForCompletion(true);

        Job job3 = Job.getInstance(conf);
        job3.setJarByClass(Friends.class);
        job3.setMapperClass(Mapper3.class);
        job3.setReducerClass(Reducer3.class);
        job3.setMapOutputKeyClass(NewFriend.class);
        job3.setMapOutputValueClass(NullWritable.class);
        job3.setPartitionerClass(Partitioner3.class);
        job3.setGroupingComparatorClass(Comparator3.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job3, new Path(otherArgs.get(1) + "/step2"));
        FileOutputFormat.setOutputPath(job3, new Path(otherArgs.get(1) + "/step3"));
        job3.waitForCompletion(true);

        Job job4 = Job.getInstance(conf);
        job4.setJarByClass(Friends.class);
        job4.setMapperClass(Mapper4.class);
        job4.setReducerClass(Reducer4.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job4, new Path(otherArgs.get(1) + "/step3"));
        FileOutputFormat.setOutputPath(job4, new Path(otherArgs.get(1) + "/final"));
        job4.waitForCompletion(true);
    }
}