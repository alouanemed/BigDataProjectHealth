package siia.alouane;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Phase 3:
 *   - Reads Phase 2 labeled data:
 *       region, yearMonth, billingAmount, zScore, isAnomaly
 *   - Filters to only isAnomaly=true
 *   - Aggregates count of anomalies per region
 *   - Writes: region, anomalyCount
 */
public class Phase3AnomalyAggregator {

    public static class AnomalyMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final Logger LOG = Logger.getLogger(AnomalyMapper.class.getName());
        private static final IntWritable ONE = new IntWritable(1);

        // Reuse your regex approach from Phase 1
        private static final Pattern CSV_PATTERN = Pattern.compile(
                "\"([^\"]*)\"|(?<=,|^)([^,]*)(?=,|$)");

        private MultipleOutputs<Text, IntWritable> multipleOutputs;

        @Override
        protected void setup(Context context) {
            multipleOutputs = new MultipleOutputs<>(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            context.getCounter("PHASE3", "TOTAL_LINES").increment(1);

            // Parse the line (region, yearMonth, billingAmount, zScore, isAnomaly)
            List<String> fields = parseCSV(value.toString());
            if (fields.size() < 5) {
                context.getCounter("PHASE3", "MALFORMED_LINE").increment(1);
                // (Optional) store invalid lines in a separate file:
                multipleOutputs.write("invalidPhase3", new Text(value.toString()), new IntWritable(0));
                return;
            }

            try {
                String region = fields.get(0).trim();
                String isAnomalyStr = fields.get(4).trim();       // "true" or "false"

                if (isAnomalyStr.equalsIgnoreCase("true")) {
                    // We found an anomaly => emit (region, 1)
                    context.write(new Text(region), ONE);
                }
            } catch (Exception e) {
                context.getCounter("PHASE3", "UNEXPECTED_ERROR").increment(1);
                LOG.warning("Error in map: " + e.getMessage());
                multipleOutputs.write("invalidPhase3", new Text(value.toString()), new IntWritable(0));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }

        /**
         * parseCSV function copied from Phase 1 style
         */
        private List<String> parseCSV(String line) {
            List<String> fields = new ArrayList<>();
            Matcher matcher = CSV_PATTERN.matcher(line);
            while (matcher.find()) {
                if (matcher.group(1) != null) {
                    fields.add(matcher.group(1)); // Quoted field
                } else {
                    fields.add(matcher.group(2)); // Unquoted field
                }
            }
            return fields;
        }
    }

    public static class AnomalyReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            // Output: region, sumOfAnomalies
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: Phase3AnomalyAggregator <phase2_input> <phase3_output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Phase3 - Anomaly Aggregator");

        // If you want a separate named output for invalid lines
        MultipleOutputs.addNamedOutput(job, "invalidPhase3", TextOutputFormat.class, Text.class, IntWritable.class);

        job.setJarByClass(Phase3AnomalyAggregator.class);
        job.setMapperClass(AnomalyMapper.class);
        job.setReducerClass(AnomalyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
