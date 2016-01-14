package flightsByCarrier;

// 1
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FlightsByCarrier {

	public static void main(String[] args) throws Exception {

		System.out.println("InputPath:" + new Path(args[0]).toString());
		System.out.println("OutputPath:" + new Path(args[1]).toString());

		// 2
		Job job = new Job();
		job.setJarByClass(FlightsByCarrier.class);
		job.setJobName("FlightsByCarrier");

		// 3
		TextInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);

		// 4
		job.setMapperClass(FlightsByCarrierMapper.class);
		job.setReducerClass(FlightsByCarrierReducer.class);

		// 5
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 6
		job.waitForCompletion(true);
	}
}
