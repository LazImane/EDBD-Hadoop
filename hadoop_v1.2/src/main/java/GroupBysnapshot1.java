
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

import javax.print.attribute.TextSyntax;

public class GroupBysnapshot1 {
    private static final String INPUT_PATH = "input-snapshot/";
    private static final String OUTPUT_PATH = "output/snapshot-";
    private static final Logger LOG = Logger.getLogger(GroupBy.class.getName());

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

    public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable>  {
        private final static DoubleWritable one = new DoubleWritable(1.0);
        private final static String emptyWords[] = { "" };

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException, NumberFormatException{
            String line = value.toString().trim();

            // Une méthode pour créer des messages de log
            //LOG.info("MESSAGE INFO");
            if (key.get() == 0) {
                LOG.info("Skipping header: " + line);
                return;
            }

            String[] columns = line.split("\\,");

            //String customerid = columns[5].trim(); //extraire seulement customer ID
            //String profit = columns[20].trim();
            // 2 et 11
            String merchant = columns[3].trim();
            String revenueinstring = columns[4].trim();
            if (Arrays.equals(columns, emptyWords))
                return;
            double revenue = Double.parseDouble(revenueinstring);





            context.write(new Text(merchant),  new DoubleWritable(revenue));

        }
    }

    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;

            for (DoubleWritable val : values)
                sum += val.get();
            //faire la somme des revenues par merchant :
            context.write(key, new DoubleWritable(sum));

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "GroupBy");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

        job.waitForCompletion(true);
    }
}
