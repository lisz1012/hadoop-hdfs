package com.lisz.hadoop.mapreduce.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class FofMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final Text names = new Text();
	private static final IntWritable ZERO = new IntWritable(0);
	private static final IntWritable ONE = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] strs = value.toString().split("\\s+");
		String myself = strs[0];
		Arrays.sort(strs);
		for (int i = 0; i < strs.length - 1; i++) {
			for(int j = i + 1; j < strs.length; j++) {
				names.set(strs[i] + " " + strs[j]);
				if (myself.equals(strs[i]) || myself.equals(strs[j])) {
					context.write(names, ZERO);
				} else {
					context.write(names, ONE);
				}
			}
		}
	}
}
