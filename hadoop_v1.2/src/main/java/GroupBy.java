
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

public class GroupBy {
	private static final String INPUT_PATH = "input-groupBy/";
	private static final String OUTPUT_PATH = "output/groupBy-";
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

	public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable>{

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO: à compléter
            String line = value.toString();
            if (key.get() == 0) return;
            String[] fields = line.split(",");

            String customerId = fields[5];
            double profit = Double.parseDouble(fields[20]);

            double sales =  Double.parseDouble(fields[fields.length-4]);
            //System.out.println("Sales: " + sales);
            String date = fields[2];
            String state = fields[10];
            String category = fields[14];

            String orderId = fields[1];
            String produitId = fields[13];
            int quantite = Integer.parseInt(fields[fields.length-3]);
            /*Exercice 2*/
           // context.write(new Text(customerId), new DoubleWritable(profit));
            String keyDate_State = date + "|" + state;
            String keyDate_Category = date + "|" + category;
            //context.write(new Text(keyDate_Category), new DoubleWritable(sales));
            /*Exercice 3.3*/
            context.write(new Text(orderId + "|" + produitId), new DoubleWritable(quantite));

		}
	}

	public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0.0;
            double totalQuantity = 0;
			for (DoubleWritable val : values) {
				sum += val.get();
                totalQuantity += val.get();
			}
            context.write(key, new DoubleWritable(totalQuantity));

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