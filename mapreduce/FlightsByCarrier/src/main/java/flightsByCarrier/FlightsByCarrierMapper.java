package flightsByCarrier;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//import au.com.bytecode.opencsv.CSVParser;

public class FlightsByCarrierMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	// @@3
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// @@4
		if (key.get() > 0) {
			// String[] lines = new CSVParser().parseLine(value.toString());
			String[] lines = value.toString().split(",");
			// @@5
			context.write(new Text(lines[8]), new IntWritable(1));
		}
	}
}