package com.lisz.hadoop.mapreduce.fof;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MyFof {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(true);
		String other[] = new GenericOptionsParser(conf, args).getRemainingArgs();
		Path inPath = new Path(other[0]);
		Path outPath = new Path(other[1]);
		if (outPath.getFileSystem(conf).exists(outPath)) outPath.getFileSystem(conf).delete(outPath, true);

		Job job = Job.getInstance(conf);
		TextInputFormat.addInputPath(job, inPath);
		TextOutputFormat.setOutputPath(job, outPath);
		job.setJobName("FOF");
		job.setJarByClass(MyFof.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		//job.setSortComparatorClass();
		job.setMapperClass(FofMapper.class);
		//job.setPartitionerClass();

		//job.setGroupingComparatorClass();// 就是按照默认的key：两个人的名字排序后的组合，所以不用分组比较器
		job.setReducerClass(FofReducer.class);

		job.waitForCompletion(true);
	}

}
