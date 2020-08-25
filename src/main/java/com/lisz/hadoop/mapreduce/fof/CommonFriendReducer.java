package com.lisz.hadoop.mapreduce.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CommonFriendReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private final Text name1 = new Text();
	private final IntWritable intWritable = new IntWritable();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable i : values) {
			if (i.get() == 0) return;
			sum += 1;
		}
		intWritable.set(sum);
		context.write(key, intWritable);
	}
}
