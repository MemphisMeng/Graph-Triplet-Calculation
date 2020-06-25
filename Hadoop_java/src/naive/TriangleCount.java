package naive;

import java.io.IOException;
import java.lang.InterruptedException;
import java.io.DataInput;
import java.io.DataOutput;
import java.nio.MappedByteBuffer;
import java.nio.file.FileSystem;
import java.util.*;

import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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

    private static List<String> edges = new ArrayList<>();
    public static class Mapper1 extends Mapper<LongWritable, Text, LongWritable, LongWritable>
    {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String tokens[] = value.toString().split("\t");
            if (tokens == null || tokens.length < 2)
                return;
            if (Long.parseLong(tokens[0]) < Long.parseLong(tokens[1])) {
                // output the edge set
                edges.add(tokens[0] + ", " + tokens[1]);
                context.write(new LongWritable(Long.parseLong(tokens[0])), new LongWritable(Long.parseLong(tokens[1])));
            }
        }
    }

    public static class Reducer1 extends Reducer<LongWritable, LongWritable, LongWritable, IntWritable>
    {
        List<Long> neighbors = new ArrayList<>();
        int sum = 0;
        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
        {
            Iterator<LongWritable> iterator = values.iterator();

            while (iterator.hasNext()) {
                neighbors.add(iterator.next().get());
            }

            for (int i = 0; i < neighbors.size(); i++) {
                for (int j = i+1; j < neighbors.size(); j++) {
                    if (edges.contains(neighbors.get(i) + ", " + neighbors.get(j)))
                        sum += 1;
                }
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException
        {
            if (sum > 0)
                context.write(new LongWritable(0), new IntWritable(sum));
        }
    }

    public int run(String[] args) throws Exception
    {
        Configuration conf = getConf();
        Job job = new Job(conf);
        job.setJobName("naive");
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setJarByClass(TriangleCount.class);
        job.setMapperClass(Mapper1.class);
        job.setReducerClass(Reducer1.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TriangleCount(), args);
        System.exit(res);
    }
}
