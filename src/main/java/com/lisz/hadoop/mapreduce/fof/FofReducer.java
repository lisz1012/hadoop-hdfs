package com.lisz.hadoop.mapreduce.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FofReducer extends Reducer<Text, IntWritable, Text, Text> {
	private final Text name1 = new Text();
	private final Text name2 = new Text();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		for (IntWritable i : values) {
			if (i.get() == 0) return; // 已经是直接好友了，就不推荐了
		}
		String strs[] = key.toString().split("\\s+");
		name1.set(strs[0]);
		name2.set(strs[1]);
		context.write(name1, name2);
	}
}
