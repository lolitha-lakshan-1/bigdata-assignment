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

public class InDegreeJob1Hadoop {

    public static class InDegreeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private final Text outKey = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.length() == 0) {
                return;
            }
            if (line.charAt(0) == '#') {
                return;
            }
            String[] parts = line.split("\\s+");
            if (parts.length < 2) {
                return;
            }
            String dst = parts[1];
            if (dst.length() == 0) {
                return;
            }
            outKey.set(dst);
            context.write(outKey, ONE);
        }
    }

    public static class InDegreeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable outValue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            outValue.set(sum);
            context.write(key, outValue);
        }
    }

    public static boolean run(Configuration baseConfiguration, String inputPath, String outputPath) throws Exception {
        Configuration conf = new Configuration(baseConfiguration);
        Job job = Job.getInstance(conf, "indegree-job1");
        job.setJarByClass(InDegreeJob1Hadoop.class);

        job.setMapperClass(InDegreeMapper.class);
        job.setReducerClass(InDegreeReducer.class);
        job.setCombinerClass(InDegreeReducer.class);

	// Performance Improvements
	//job.setNumReduceTasks(2);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true);
    }
}
