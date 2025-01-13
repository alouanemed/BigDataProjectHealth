package siia.alouane;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Phase 2:
 * - Loads baseline CSV (region,yearMonth,mean,stdDev) from Phase 1.
 * - Reads the same or new dataset.
 * - Groups by (region|yearMonth) to find baseline, computes zScore, flags anomalies.
 * - Outputs CSV: region,yearMonth,billingAmount,zScore,isAnomaly
 */
public class Phase2AnomalyLabeling {

    public static class AnomalyMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

        private static final Logger LOG = Logger.getLogger(AnomalyMapper.class.getName());
        private final Map<String, Stats> baselineMap = new HashMap<>();
        private static final Pattern CSV_PATTERN = Pattern.compile(
                "\"([^\"]*)\"|(?<=,|^)([^,]*)(?=,|$)");

        private static class Stats {
            double mean;
            double stdDev;
            Stats(double mean, double stdDev) {
                this.mean = mean;
                this.stdDev = stdDev;
            }
        }

        private static final double ZSCORE_THRESHOLD = 2.0;

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String baselinePath = conf.get("phase1.output.path");
            if (baselinePath == null) {
                throw new IOException("Missing 'phase1.output.path' in config.");
            }

            LOG.info("Loading baseline from: " + baselinePath);
            Path path = new Path(baselinePath);
            FileSystem fs = FileSystem.get(conf);

            if (!fs.exists(path)) {
                throw new IOException("Baseline file not found: " + baselinePath);
            }

            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                String line;
                while ((line = br.readLine()) != null) {
                    List<String> fields = parseCSV(line);
                    if (fields.size() < 4) {
                        continue; // Skip malformed baseline lines
                    }
                    String region = fields.get(0).trim();
                    String yearMonth = fields.get(1).trim();
                    double mean = Double.parseDouble(fields.get(2).trim());
                    double stdDev = Double.parseDouble(fields.get(3).trim());

                    String key = region + "|" + yearMonth;
                    baselineMap.put(key, new Stats(mean, stdDev));
                }
            }
            LOG.info("Loaded " + baselineMap.size() + " baseline entries.");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            context.getCounter("PHASE2", "TOTAL_LINES").increment(1);

            List<String> fields = parseCSV(value.toString());
            if (fields.size() < 17) {
                context.getCounter("PHASE2", "MALFORMED_LINE").increment(1);
                return;
            }

            try {
                String dateOfAdmission = fields.get(5).trim();
                String billingAmountStr = fields.get(9).trim();
                String region = fields.get(16).trim();

                if (region.isEmpty() || dateOfAdmission.length() < 7) {
                    context.getCounter("PHASE2", "INVALID_FIELDS").increment(1);
                    return;
                }

                String yearMonth = dateOfAdmission.substring(0, 7);
                double billingAmount = Double.parseDouble(billingAmountStr);

                String mapKey = region + "|" + yearMonth;
                Stats stats = baselineMap.get(mapKey);
                if (stats == null) {
                    context.getCounter("PHASE2", "NO_BASELINE_FOUND").increment(1);
                    return;
                }

                if (stats.stdDev == 0.0) {
                    context.getCounter("PHASE2", "ZERO_STD_DEV").increment(1);
                    writeOutput(context, region, yearMonth, billingAmount, 0.0, true);
                    return;
                }

                double zScore = (billingAmount - stats.mean) / stats.stdDev;
                boolean isAnomaly = (Math.abs(zScore) >= ZSCORE_THRESHOLD);
                if (isAnomaly) {
                    context.getCounter("PHASE2", "ANOMALIES").increment(1);
                }

                writeOutput(context, region, yearMonth, billingAmount, zScore, isAnomaly);

            } catch (NumberFormatException e) {
                context.getCounter("PHASE2", "INVALID_BILLING_AMOUNT").increment(1);
            }
        }

        private void writeOutput(Context context,
                                 String region,
                                 String yearMonth,
                                 double billingAmount,
                                 double zScore,
                                 boolean isAnomaly)
                throws IOException, InterruptedException {

            String output = String.format("%s,%s,%.6f,%.6f,%b",
                    region, yearMonth, billingAmount, zScore, isAnomaly);
            context.write(NullWritable.get(), new Text(output));
        }

        private List<String> parseCSV(String line) {
            List<String> fields = new ArrayList<>();
            Matcher matcher = CSV_PATTERN.matcher(line);
            while (matcher.find()) {
                if (matcher.group(1) != null) {
                    fields.add(matcher.group(1));
                } else {
                    fields.add(matcher.group(2));
                }
            }
            return fields;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: Phase2AnomalyLabeling <input> <output> <baselineCSV>");
            System.exit(2);
        }

        String inputPath = args[0];
        String outputPath = args[1];
        String baselinePath = args[2];

        Configuration conf = new Configuration();
        conf.set("phase1.output.path", baselinePath);

        Job job = Job.getInstance(conf, "Phase2 - RegionMonth Anomaly Labeling");
        job.setJarByClass(Phase2AnomalyLabeling.class);

        job.setMapperClass(AnomalyMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
