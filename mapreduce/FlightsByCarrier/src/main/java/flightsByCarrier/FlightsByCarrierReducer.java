package flightsByCarrier;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlightsByCarrierReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	// @@3
	@Override
	protected void reduce(Text token, Iterable<IntWritable> counts, Context context)
			throws IOException, InterruptedException {
		int sum = 0;

		// @@4
		for (IntWritable count : counts) {
			sum += count.get();
		}
		// @@5
		context.write(token, new IntWritable(sum));
	}
}