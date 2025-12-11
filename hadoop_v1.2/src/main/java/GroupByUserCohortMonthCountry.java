
import java.io.IOException;
import java.time.Instant;
import java.util.HashSet;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class GroupByUserCohortMonthCountry {
    private static final String INPUT_PATH = "input-groupBy/user_month_country.csv";
    private static final String OUTPUT_PATH = "output/groupBy-UserCohortMonthCountry-";
    private static final Logger LOG = Logger.getLogger(GroupByUserCohortMonthCountry.class.getName());

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%n%6$s");

        try {
            FileHandler fh = new FileHandler("out.log");
            fh.setFormatter(new SimpleFormatter());
            LOG.addHandler(fh);
        } catch (SecurityException | IOException e) {
            System.exit(1);
        }
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text>{

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            String[] parts = line.split("\t");

            String userCohortMonthCountryKey = parts[0]; //User|CohortMonth|Country
            String[] fields = userCohortMonthCountryKey.split("\\|");
            String user = fields[0];
            String month = fields[1];
            String country = fields[2];
            int revenue = Integer.parseInt(parts[1]); //conversion_value

            context.write(new Text(country + "|" + month), new Text(user + "," + revenue));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            HashSet<String> uniqueUsers = new HashSet<>();
            double totalRevenue = 0;

            for (Text val : values) {
                String[] p = val.toString().split(",");
                String userId = p[0];
                double rev = Double.parseDouble(p[1]);

                uniqueUsers.add(userId);
                totalRevenue += rev;
            }

            int distinctUserCount = uniqueUsers.size();

            context.write(
                    key,
                    new Text(distinctUserCount + "\t" + totalRevenue)
            );

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "GroupByUserCohortMonthCountry");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

        job.waitForCompletion(true);
    }
}