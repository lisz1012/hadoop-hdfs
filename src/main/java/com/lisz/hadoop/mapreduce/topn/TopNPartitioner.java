package com.lisz.hadoop.mapreduce.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

// 分区器的计算不能太复杂， 相同年月的返回值相同就可以了 （暂不考虑这么些造成的数据倾斜）
public class TopNPartitioner extends Partitioner<TopNKey, IntWritable> {
	@Override
	public int getPartition(TopNKey topNKey, IntWritable intWritable, int numPartitions) {
		return (topNKey.getYear() + topNKey.getMonth()) % numPartitions;
	}
}
