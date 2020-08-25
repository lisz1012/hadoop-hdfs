package com.lisz.hadoop.practice2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TwoDaysOfHighestTemperature {
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration((true));
		String other[] = new GenericOptionsParser(conf, args).getRemainingArgs();
		Path inPath = new Path(other[0]);
		Path outPath = new Path(other[1]);
		if (outPath.getFileSystem(conf).exists(outPath)) outPath.getFileSystem(conf).delete(outPath, true);

		Job job = Job.getInstance(conf);
		TextInputFormat.addInputPath(job, inPath);
		TextOutputFormat.setOutputPath(job, outPath);
		job.setJarByClass(TwoDaysOfHighestTemperature.class);
		job.setJobName("TwoDaysOfHighestTemperature");

		job.setMapOutputKeyClass(TwoDaysOfHighestTemperatureKey.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setMapperClass(TwoDaysOfHighestTemperatureMapper.class);
		job.setPartitionerClass(TwoDaysOfHighestTemperaturePartitioner.class);
		job.setSortComparatorClass(TwoDaysOfHighestTemperatureSortComparator.class);

		job.setGroupingComparatorClass(TwoDaysOfHighestTemperatureGroupingComparator.class);
		job.setReducerClass(TwoDaysOfHighestTemperatureReducer.class);

		job.waitForCompletion(true);
	}
}
