package test5;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class WCMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	public static final IntWritable one = new IntWritable(1);
	
	public void map(LongWritable valuess, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter ) throws IOException{
		String longValue = value.toString();
		String[] values = longValue.split("-");
		output.collect(new Text(values[0])	, one);
	}

}

