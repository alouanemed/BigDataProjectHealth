package siia.alouane;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Phase1RegionBaselineComputation {

    public static class BaselineMapper
            extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private static final Logger LOG = Logger.getLogger(BaselineMapper.class.getName());
        private static final Pattern CSV_PATTERN = Pattern.compile(
                "\"([^\"]*)\"|(?<=,|^)([^,]*)(?=,|$)");

        private final Text outKey = new Text();
        private final DoubleWritable outValue = new DoubleWritable();
        private MultipleOutputs<Text, DoubleWritable> multipleOutputs;

        @Override
        protected void setup(Context context) {
            // Initialize MultipleOutputs
            multipleOutputs = new MultipleOutputs<>(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            context.getCounter("PHASE1", "TOTAL_LINES").increment(1);

            List<String> fields = parseCSV(value.toString());
            if (fields.size() < 17) {
                context.getCounter("PHASE1", "MALFORMED_LINE").increment(1);
                multipleOutputs.write("invalid", new Text(value.toString()), new DoubleWritable(0));
                return;
            }

            try {
                String dateOfAdmission = fields.get(5).trim();
                String billingAmountStr = fields.get(9).trim();
                String region = fields.get(16).trim();

                if (region.isEmpty() || dateOfAdmission.length() < 7) {
                    context.getCounter("PHASE1", "INVALID_FIELDS").increment(1);
                    multipleOutputs.write("invalid", new Text(value.toString()), new DoubleWritable(0));
                    return;
                }

                // Extract year-month, e.g. "2024-01"
                String yearMonth = dateOfAdmission.substring(0, 7);

                // Parse billingAmount as double (allows negative, decimal)
                double billingAmount = Double.parseDouble(billingAmountStr);

                // Build grouping key: region|YYYY-MM
                String groupKey = region + "|" + yearMonth;

                outKey.set(groupKey);
                outValue.set(billingAmount);
                context.write(outKey, outValue);

            } catch (NumberFormatException e) {
                context.getCounter("PHASE1", "INVALID_BILLING_AMOUNT").increment(1);
                multipleOutputs.write("invalid", new Text(value.toString()), new DoubleWritable(0));
            } catch (Exception e) {
                context.getCounter("PHASE1", "UNEXPECTED_ERROR").increment(1);
                LOG.warning("Unexpected error: " + e.getMessage());
                multipleOutputs.write("invalid", new Text(value.toString()), new DoubleWritable(0));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Close MultipleOutputs
            multipleOutputs.close();
        }

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

    public static class BaselineReducer
            extends Reducer<Text, DoubleWritable, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0.0;
            double sumOfSquares = 0.0;
            long count = 0;

            for (DoubleWritable val : values) {
                double amount = val.get();
                sum += amount;
                sumOfSquares += amount * amount;
                count++;
            }

            if (count == 0) {
                return;
            }

            double mean = sum / count;
            double variance = (sumOfSquares / count) - (mean * mean);
            if (variance < 0) {
                variance = 0; // numeric stability
            }
            double stdDev = Math.sqrt(variance);

            // key = "region|yearMonth"
            String[] parts = key.toString().split("\\|", -1);
            if (parts.length < 2) {
                context.getCounter("PHASE1", "REDUCER_KEY_ERROR").increment(1);
                return;
            }

            String region = parts[0];
            String yearMonth = parts[1];

            // Output CSV: region,yearMonth,mean,stdDev
            String output = String.format("%s,%s,%.6f,%.6f",
                    region, yearMonth, mean, stdDev);

            // Write null as key so output is a single CSV column
            context.write(null, new Text(output));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: Phase1RegionBaselineComputation <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Phase1 - RegionMonth Baseline");

        MultipleOutputs.addNamedOutput(job, "invalid", TextOutputFormat.class, Text.class, Text.class);

        job.setJarByClass(Phase1RegionBaselineComputation.class);
        job.setMapperClass(BaselineMapper.class);
        job.setReducerClass(BaselineReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
