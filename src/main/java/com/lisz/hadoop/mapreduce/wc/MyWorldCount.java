package com.lisz.hadoop.mapreduce.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// 在右侧的maven中LifeCycle -> clean、package之后，上传到hadoop集群的某一台机器上，然后
// hadoop jar ./hadoop-hdfs-1.0-SNAPSHOT.jar com.lisz.hadoop.mapreduce.wc.MyWorldCount
// 其中，输入输出路径已经硬编码写在了代码里
// 命令运行程序的时候：hadoop jar xxx.jar -D blocksize=1048576 inpath outpath 中 -D blocksize=1048576是全局配置属性
// inpath outpath是命令自己需要的属性。所以在args中可以见到全局的blocksize，和与框架无关的inpath outpath之类的参数。作为
// 人的复杂度：自己分析agrs数组
// https://hadoop.apache.org/docs/r2.10.0/hadoop-mapreduce-client/hadoop-mapreduce-client-core/mapred-default.xml
// 中提到的参数的操作见 org.apache.hadoop.mapreduce.MRJobConfig 也就是说-D 后面可以接这些变量名
public class MyWorldCount {
	public static void main(String[] args) throws Exception{
		// 默认加载编译完了之后的目录下的各个xml配置文件
		Configuration conf = new Configuration(true);

		GenericOptionsParser parser = new GenericOptionsParser(conf, args); // 工具类帮我们把通用的-D类的属性直接set到conf里，留下commandOptions
		String otherArgs[] = parser.getRemainingArgs(); // 这里就可以取出输入输出路径了

		// 让框架知道是异构平台运行, 以不传Jar包的方式直接运行main方法的时候用这一句
		conf.set("mapreduce.app-submission.cross-platform", "true");
		// 单机运行， 此时不写下面job.setJar那一句
		/*System.out.println(conf.get("mapreduce.framework.name"));
		conf.set("mapreduce.framework.name", "local");
		System.out.println(conf.get("mapreduce.framework.name"));*/
		Job job = Job.getInstance(conf);
		// 客户端会上传这个指定了的jar包，然后集群就会去下载放到classpath，然后就可以找到MyMapper和MyReducer类了。本地触发，发送到集群运行的时候需要这一句
		job.setJar("/Users/shuzheng/IdeaProjects/hadoop-hdfs/target/hadoop-hdfs-1.0-SNAPSHOT.jar");
		// 必须写，自己这个类. 反推这个类归属于哪个Jar包
		job.setJarByClass(MyWorldCount.class);
		job.setJobName("myJob");
		Path infile = new Path(otherArgs[0]);
		TextInputFormat.addInputPath(job, infile);
		Path outfile = new Path(otherArgs[1]);
		if (outfile.getFileSystem(conf).exists(outfile)) {
			outfile.getFileSystem(conf).delete(outfile, true);
		}
		TextOutputFormat.setOutputPath(job, outfile);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);  // 为了反射变成对象
		job.setMapOutputValueClass(IntWritable.class); // 为了反射变成对象
		job.setReducerClass(MyReducer.class);
		//job.setNumReduceTasks(5);  // 这里再设置的话，会覆盖args里传过来的
		job.waitForCompletion(true);
	}
}
