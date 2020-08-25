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
		for (int i = 1; i < strs.length; i++) {
			names.set(getKeyForNames(myself, strs[i]));
			context.write(names, ZERO);
		}
		for (int i = 1; i < strs.length - 1; i++) {
			for(int j = i + 1; j < strs.length; j++) {
				names.set(getKeyForNames(strs[i], strs[j]));
				context.write(names, ONE);
			}
		}
	}

	private String getKeyForNames(String s1, String s2) {
		if (s1.compareTo(s2) < 0) {
			return s1 + " " + s2;
		} else {
			return s2 + " " + s1;
		}
	}
}
