package com.lisz.hadoop.practice2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TwoDaysOfHighestTemperaturePartitioner extends Partitioner<TwoDaysOfHighestTemperatureKey, IntWritable> {
	@Override
	public int getPartition(TwoDaysOfHighestTemperatureKey twoDaysOfHighestTemperatureKey, IntWritable intWritable, int numPartitions) {
		return (twoDaysOfHighestTemperatureKey.getYear() + twoDaysOfHighestTemperatureKey.getMonth()) % numPartitions;
	}
}
