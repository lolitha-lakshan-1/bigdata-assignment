import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class InDegreeJob2Hadoop {

    public static class DistributionMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private final IntWritable outKey = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.length() == 0) {
                return;
            }
            String[] parts = line.split("\\s+");
            if (parts.length < 2) {
                return;
            }
            String degreeString = parts[1];
            if (degreeString.length() == 0) {
                return;
            }
            int degree;
            try {
                degree = Integer.parseInt(degreeString);
            } catch (NumberFormatException e) {
                return;
            }
            outKey.set(degree);
            context.write(outKey, ONE);
        }
    }

    public static class DistributionReducer extends Reducer<IntWritable, IntWritable, Text, IntWritable> {
        private final Text outKey = new Text();
        private final IntWritable outValue = new IntWritable();

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            outKey.set(Integer.toString(key.get()));
            outValue.set(sum);
            context.write(outKey, outValue);
        }
    }

    public static boolean run(Configuration baseConfiguration, String inputPath, String outputPath) throws Exception {
        Configuration conf = new Configuration(baseConfiguration);
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf, "indegree-job2-distribution");
        job.setJarByClass(InDegreeJob2Hadoop.class);

        job.setMapperClass(DistributionMapper.class);
        job.setReducerClass(DistributionReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setNumReduceTasks(1);

        return job.waitForCompletion(true);
    }
}
