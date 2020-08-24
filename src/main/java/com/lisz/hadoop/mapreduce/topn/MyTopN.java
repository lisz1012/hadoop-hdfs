package com.lisz.hadoop.mapreduce.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MyTopN {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(true);
		String[] other = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf);
		job.setJarByClass(MyTopN.class);
		job.setJobName("myTopN");

		// 作为初学者，关注的是Client端的代码梳理，下面的代码要熟练, 如果把这块写明白了，也就真的知道这个作业的开发原理了
		// MR的作者屏蔽了很多技术底层的细节，我们只需要关心和业务相关的点就好
		// map
		TextInputFormat.addInputPath(job, new Path(other[0]));
		Path outPath = new Path(other[1]);
		if (outPath.getFileSystem(conf).exists(outPath))  outPath.getFileSystem(conf).delete(outPath, true);
		TextOutputFormat.setOutputPath(job, outPath);
		job.setMapOutputKeyClass(TopNKey.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setMapperClass(TopNMapper.class);
		job.setPartitionerClass(TopNPartitioner.class); // 分区 > 分组。甚至还可以按年分区。分区器的潜台词：相同的key获得相同的分区号
		job.setSortComparatorClass(TopNSortComparator.class);// 按照：年-月 温度（倒序）排序
		//job.setCombinerClass(TopNCombiner.class);

		// reduce
		job.setGroupingComparatorClass(TopNGroupingComparator.class);
		job.setReducerClass(TopNReducer.class);


		job.waitForCompletion(true);
	}
}
