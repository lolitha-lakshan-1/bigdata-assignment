import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class InDegreeSpark {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: InDegreeSpark <datasetName>");
            System.exit(1);
        }

        String datasetName = args[0];
        String localInputPath = "/data/" + datasetName + ".txt";
        String localOutputPath = "/tmp/indegree/" + datasetName + "-indegree-spark.csv";

        SparkConf conf = new SparkConf().setAppName("InDegreeSpark-" + datasetName);
        JavaSparkContext sc = new JavaSparkContext(conf);

        try {
            JavaRDD<String> lines = sc.textFile("file://" + localInputPath);

            JavaRDD<String> cleaned = lines.filter(line -> {
                if (line == null) {
                    return false;
                }
                String trimmed = line.trim();
                if (trimmed.length() == 0) {
                    return false;
                }
                return trimmed.charAt(0) != '#';
            });

            JavaPairRDD<String, Integer> dstOnes = cleaned.mapToPair(line -> {
                String trimmed = line.trim();
                String[] parts = trimmed.split("\\s+");
                if (parts.length < 2) {
                    return new Tuple2<String, Integer>("", 0);
                }
                String dst = parts[1];
                if (dst == null || dst.length() == 0) {
                    return new Tuple2<String, Integer>("", 0);
                }
                return new Tuple2<String, Integer>(dst, 1);
            }).filter(t -> t._1().length() > 0 && t._2() != 0);

            JavaPairRDD<String, Integer> indegreeByNode = dstOnes.reduceByKey((a, b) -> a + b);

            JavaPairRDD<Integer, Integer> degreeCountOnes = indegreeByNode
                    .mapToPair(t -> new Tuple2<Integer, Integer>(t._2(), 1));

            JavaPairRDD<Integer, Integer> degreeCounts = degreeCountOnes.reduceByKey((a, b) -> a + b);

            JavaPairRDD<Integer, Integer> sortedDegreeCounts = degreeCounts.sortByKey(true);

            JavaRDD<String> outputLines = sortedDegreeCounts.map(t -> t._1() + "," + t._2());

            List<String> collected = outputLines.collect();

            writeLocalFile(localOutputPath, collected);
        } finally {
            sc.stop();
        }
    }

    private static void writeLocalFile(String path, List<String> lines) {
        File file = new File(path);
        File parent = file.getParentFile();
        if (parent != null && !parent.exists()) {
            if (!parent.mkdirs()) {
                System.err.println("Failed to create parent directories for " + path);
            }
        }
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(file, false));
            for (String line : lines) {
                writer.write(line);
                writer.newLine();
            }
            writer.flush();
        } catch (IOException e) {
            System.err.println("Error writing output file " + path);
            e.printStackTrace(System.err);
            System.exit(1);
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException ignored) {
                }
            }
        }
    }
}
