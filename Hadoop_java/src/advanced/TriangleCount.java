package advanced;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TriangleCount extends Configured implements Tool{

    //we need to record the node set
    private static List<Long> nodes = new ArrayList<>();
    //we need to record the edge set
    private static List<String> edges = new ArrayList<>();
    public static class Mapper1 extends Mapper<LongWritable, Text, LongWritable, LongWritable>
    {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            long e1, e2;
            if (tokenizer.hasMoreTokens()) {
                e1 = Long.parseLong(tokenizer.nextToken());
                if (!tokenizer.hasMoreTokens())
                    throw new RuntimeException("There is only one node in this line " + line);
                e2 = Long.parseLong(tokenizer.nextToken());

                edges.add(e1 + ", " + e2);
                if (!nodes.contains(e1))
                    nodes.add(e1);
                if (!nodes.contains(e2))
                    nodes.add(e2);
                // Make sure each edge counts only once
                if (e1 < e2) {
                    context.write(new LongWritable(e1), new LongWritable(e2));
                }
            }
        }
    }

    public static class Reducer1 extends Reducer<LongWritable, LongWritable, Text, Text>
    {

        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
        {
            List<Long> neighbors = new ArrayList<>();
            Iterator<LongWritable> iterator = values.iterator();
            while (iterator.hasNext()) {
                long e = iterator.next().get();
                neighbors.add(e);
            }

            for (int i = 0; i < neighbors.size(); i++) {
                for (int j = i + 1; j < neighbors.size(); j++) {
                    // Type 1st output: <v, (u, w)>
                    context.write(new Text(key.toString()), new Text(neighbors.get(i) + ", " + neighbors.get(j)));
                }
            }

            for (long node: nodes) {
                if (key.get() != node) {
                    if (edges.contains(key.get() + ", " + node)) {
                        // both nodes are connected directly with each other
                        // Type 2nd output: <(u, w), $>
                        // I use -1 to represent $ so that it is uniform
                        context.write(new Text(key.get() + ", " + node), new Text(String.valueOf(-1)));
                    }
                }
            }
        }
    }

    public static class Mapper2 extends Mapper<LongWritable, Text, Text, LongWritable>
    {
        Text mKey = new Text();
        LongWritable mValue = new LongWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            if (tokenizer.hasMoreTokens()) {
                if (tokenizer.nextToken().contains(", ")) {
                    // Type 2nd
                    mKey.set(tokenizer.nextToken());
                    if (!tokenizer.hasMoreTokens())
                        throw new RuntimeException("There is only one node in line " + line);
                    mValue.set(Long.parseLong(tokenizer.nextToken()));
                }
                else {
                    // Type 1st
                    mValue.set(Long.parseLong(tokenizer.nextToken()));
                    if (!tokenizer.hasMoreTokens())
                        throw new RuntimeException("There is only one node in line " + line);
                    mKey.set(tokenizer.nextToken());
                }
                context.write(mKey, mValue);
            }
        }
    }

    public static class Reducer2 extends Reducer<Text, LongWritable, LongWritable, LongWritable>
    {
        long count = 0;

        public void cleanup(Context context) throws IOException, InterruptedException
        {
            LongWritable v = new LongWritable(count);
            if (count > 0)
                context.write(new LongWritable(0), v);
        }

        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
        {
            List<Long> pivots = new ArrayList<>();
            Iterator<LongWritable> iterator = values.iterator();
            while (iterator.hasNext()) {
                long e = iterator.next().get();
                if (!pivots.contains(e))
                    pivots.add(e);
            }
            if (pivots.contains((long)(-1))) {
                for (long pivot: pivots) {
                    if (pivot != (long)(-1))
                        count += 1;
                }
            }
        }
    }

    public static class Mapper3 extends Mapper<LongWritable, Text, Text, LongWritable>
    {
        Text mKey = new Text();
        LongWritable mValue = new LongWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            if (tokenizer.hasMoreTokens()) {
                mKey.set(tokenizer.nextToken());
                if (!tokenizer.hasMoreTokens())
                    throw new RuntimeException("invalid intermediate line " + line);
                mValue.set(Long.parseLong(tokenizer.nextToken()));
                context.write(mKey, mValue);
            }
        }
    }

    public static class Reducer3 extends Reducer<Text, LongWritable, LongWritable, LongWritable>
    {
        long sum = 0;
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
        {
            Iterator<LongWritable> iterator = values.iterator();
            while (iterator.hasNext()) {
                sum += iterator.next().get();
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException
        {
            if (sum > 0)
                context.write(new LongWritable(0), new LongWritable(sum / 3));
        }
    }

    public int run(String[] args) throws Exception
    {
        Job job1 = new Job(getConf());
        job1.setJobName("advanced_step1");
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(LongWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setJarByClass(TriangleCount.class);
        job1.setMapperClass(Mapper1.class);
        job1.setReducerClass(Reducer1.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("temp1"));

        Job job2 = new Job(getConf());
        job2.setJobName("advanced_step2");
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(LongWritable.class);
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(LongWritable.class);
        job2.setJarByClass(TriangleCount.class);
        job2.setMapperClass(Mapper2.class);
        job2.setReducerClass(Reducer2.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2, new Path("temp1"));
        FileOutputFormat.setOutputPath(job2, new Path("temp2"));

        Job job3 = new Job(getConf());
        job3.setJobName("advanced_step3");
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(LongWritable.class);
        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(LongWritable.class);
        job3.setJarByClass(TriangleCount.class);
        job3.setMapperClass(Mapper3.class);
        job3.setReducerClass(Reducer3.class);
        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job3, new Path("temp2"));
        FileOutputFormat.setOutputPath(job3, new Path(args[1]));

        int ret = job1.waitForCompletion(true) ? 0 : 1;
        if (ret == 0)
            ret = job2.waitForCompletion(true) ? 0 : 1;
        if (ret == 0)
            ret = job3.waitForCompletion(true) ? 0 : 1;

        return ret;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TriangleCount(), args);
        System.exit(res);
    }
}
