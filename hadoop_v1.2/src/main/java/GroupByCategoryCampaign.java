
import java.io.IOException;
import java.time.Instant;
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

public class GroupByCategoryCampaign {
    private static final String INPUT_PATH = "input-groupBy/category_campaign.csv";
    private static final String OUTPUT_PATH = "output/groupBy-category-campaign";
    private static final Logger LOG = Logger.getLogger(GroupByCategoryCampaign.class.getName());

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

    public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] parts = line.split("\\s+");
            int n = parts.length;
            String categoryCampaign = parts[0];
            for( int i=1 ; i<n-1 ; i++){
                categoryCampaign += "|"+parts[i];
            }
            System.out.println("key : "+ categoryCampaign + "\n");
            double revenue = Double.parseDouble(parts[parts.length -1]);
            System.out.println("revenue" + revenue +"\n");
            context.write(new Text(categoryCampaign), new DoubleWritable(revenue));
        }
    }


    public static class Reduce extends Reducer<Text, DoubleWritable, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0;
            int count = 0;

            for (DoubleWritable v : values) {
                sum += v.get();
                count++;
            }

            String result = "total_revenue=" + sum + "\tcount=" + count;

            context.write(key, new Text(result));
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "GroupByCategoryCampaign");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

        job.waitForCompletion(true);
    }
}