package com.lisz.hadoop.mapreduce.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

// 在右侧的maven中LifeCycle -> clean、package之后，上传到hadoop集群的某一台机器上，然后
// hadoop jar ./hadoop-hdfs-1.0-SNAPSHOT.jar com.lisz.hadoop.mapreduce.wc.MyWorldCount
// 其中，输入输出路径已经硬编码写在了代码里
public class MyWorldCount {
	public static void main(String[] args) throws Exception{
		// 默认加载编译完了之后的目录下的各个xml配置文件
		Configuration conf = new Configuration(true);
		// 让框架知道是异构平台运行, 以不传Jar包的方式直接运行main方法的时候用这一句
		conf.set("mapreduce.app-submission.cross-platform", "true");
		// 单机运行， 此时不写下面job.setJar那一句
		/*System.out.println(conf.get("mapreduce.framework.name"));
		conf.set("mapreduce.framework.name", "local");
		System.out.println(conf.get("mapreduce.framework.name"));*/
		Job job = Job.getInstance(conf);
		// 客户端会上传这个指定了的jar包，然后集群就会去下载放到classpath，然后就可以找到MyMapper和MyReducer类了
		job.setJar("/Users/shuzheng/IdeaProjects/hadoop-hdfs/target/hadoop-hdfs-1.0-SNAPSHOT.jar");
		// 必须写，自己这个类. 反推这个类归属于哪个Jar包
		job.setJarByClass(MyWorldCount.class);
		job.setJobName("myJob");
		Path infile = new Path("/data/wc/input");
		TextInputFormat.addInputPath(job, infile);
		Path outfile = new Path("/data/wc/output");
		if (outfile.getFileSystem(conf).exists(outfile)) {
			outfile.getFileSystem(conf).delete(outfile, true);
		}
		TextOutputFormat.setOutputPath(job, outfile);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);  // 为了反射变成对象
		job.setMapOutputValueClass(IntWritable.class); // 为了反射变成对象
		job.setReducerClass(MyReducer.class);
		job.waitForCompletion(true);
	}
}
