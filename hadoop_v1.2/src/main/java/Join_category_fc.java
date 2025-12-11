
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Join_category_fc {
    private static final String INPUT_PATH = "input-join/";
    private static final String OUTPUT_PATH = "output/join_category_fc-";
    private static final Logger LOG = Logger.getLogger(Join_category_fc.class.getName());

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

    public static class factConversionMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (key.get() == 0) return;
            String[] fields = line.split(",");
            String category_id = fields[7];
            String conversion_value = fields[9];
            String campaign_id = fields[5];

            context.write(new Text(category_id), new Text("fact : " + conversion_value +"|"+campaign_id));
        }
    }

    public static class categoryMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //need name and Id
            String line = value.toString();
            String[] fields = line.split(",");
            String category_id = fields[0];
            String category_name = fields[1];
            context.write(new Text(category_id), new Text("category : "+ category_name));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            List<String> conversions = new ArrayList<>();
            List<String> category = new ArrayList<>();

            for (Text val : values) {
                String s = val.toString();
                if (s.startsWith("fact : ")) {
                    conversions.add(s.substring("fact : ".length()));
                } else if (s.startsWith("category : ")) {
                    category.add(s.substring("category : ".length()));
                }
            }

            //join context.write(new Text(customerName), new Text(orderComment));
            for(String value : conversions){
                for(String category_name : category){
                    String[] parts = value.split("\\|");
                    context.write(new Text(category_name + "|" + parts[1]/*campaign_id)*/), new Text(parts[0]));
                    System.out.println(category_name + "|" + parts[1] + ":" + parts[0]);
                }
            }
        }
    }


public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = new Job(conf, "Join_category_fc");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setReducerClass(Reduce.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    MultipleInputs.addInputPath(job,
            new Path("input-join/fact_conversion.csv"),
            TextInputFormat.class,
            factConversionMapper.class);

    MultipleInputs.addInputPath(job,
            new Path("input-join/dim_category.csv"),
            TextInputFormat.class,
            categoryMapper.class);

    FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

    job.waitForCompletion(true);
}
}