import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class InDegreeMainHadoop {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: InDegreeMainHadoop <datasetName>");
            System.exit(1);
        }

        String datasetName = args[0];

        String hdfsInputPath = "/input/" + datasetName + "-hadoop.txt";
        String hdfsTempPath = "/tmp/" + datasetName + "-indegree";
        String hdfsOutputPath = "/output/" + datasetName + "-indegree";
        String localOutputPath = "/data/" + datasetName + "-indegree-hadoop.csv";

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path inputPath = new Path(hdfsInputPath);
        if (!fs.exists(inputPath)) {
            System.err.println("HDFS input not found: " + hdfsInputPath);
            System.exit(1);
        }

        Path tempPath = new Path(hdfsTempPath);
        Path outputPath = new Path(hdfsOutputPath);

        if (fs.exists(tempPath)) {
            fs.delete(tempPath, true);
        }
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        long start = System.currentTimeMillis();
        System.out.println("DATASET_START " + datasetName + " " + start);

        boolean ok1 = InDegreeJob1Hadoop.run(conf, hdfsInputPath, hdfsTempPath);
        if (!ok1) {
            System.err.println("Job 1 failed for dataset " + datasetName);
            System.exit(1);
        }

        boolean ok2 = InDegreeJob2Hadoop.run(conf, hdfsTempPath, hdfsOutputPath);
        if (!ok2) {
            System.err.println("Job 2 failed for dataset " + datasetName);
            System.exit(1);
        }

        Path hdfsOutputFile = new Path(hdfsOutputPath + "/part-r-00000");
        Path localOutputFile = new Path(localOutputPath);
        fs.copyToLocalFile(false, hdfsOutputFile, localOutputFile, true);

        long end = System.currentTimeMillis();
        long duration = end - start;
        System.out.println("DATASET_END " + datasetName + " " + end + " " + duration);
    }
}
