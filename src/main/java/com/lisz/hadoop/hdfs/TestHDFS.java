package com.lisz.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

public class TestHDFS {
	public Configuration conf = null;
	public FileSystem fs;

	@Before
	public void connect() throws Exception {
		conf = new Configuration(true);
		// 参考了：core-site.xml 中的<value>hdfs://mycluster</value> hdfs 取环境变量HADOOP_USER_NAME
		// fs = FileSystem.get(conf);
		fs = FileSystem.get(URI.create("hdfs://mycluster"), conf, "god");
	}

	@Test
	public void mkdir() throws IOException {
		Path dir = new Path("/lisz01");
		if (fs.exists(dir)) {
			fs.delete(dir, true);
		}
		fs.mkdirs(dir);
	}

	@Test
	public void upload() throws Exception {
		BufferedInputStream input = new BufferedInputStream(new FileInputStream("./data/hello.txt"));
		Path outfile = new Path("/lisz/out.txt");
		FSDataOutputStream output = fs.create(outfile);
		IOUtils.copyBytes(input, output, conf, true);
	}

	@Test
	public void download() throws Exception {
		BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream("./data/hello2.txt"));
		Path outfile = new Path("/lisz/out.txt");
		FSDataInputStream input = fs.open(outfile);
		IOUtils.copyBytes(input, output, conf, true);
	}

	@Test
	public void blocks() throws Exception {
		Path file = new Path("/user/god/data.txt");
		FileStatus fileStatus = fs.getFileStatus(file);
		BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, 0L, fileStatus.getLen());
		for (BlockLocation blockLocation : fileBlockLocations) {
			System.out.println(blockLocation);
		}
		/*
		输出：
		0,1048576,hadoop-02,hadoop-04,hadoop-03
		1048576,640319,hadoop-04,hadoop-03,hadoop-02
		通过 fs.getFileStatus 和 fs.getFileBlockLocations 能拿到块的偏移量、大小和分布情况，从而很容易地计算向数据移动
		一个程序移动到第一个节点所在的块，一个程序移动到第二个节点所在的块。
		用户和程序读取的是文件这个级别，并不知道有块这个概念
		 */

		FSDataInputStream in = fs.open(file); // 面向文件打开的输入流，无论怎么读都是从文件的开始位置开始读

		/*
		blk_1073741826 (Block01)最后是hell，blk_1073741826（Block02）最开始是：o lisz 62335，可见是紧接着Block01的
		 */

		//这里指定从哪里开始读取，有的程序并不是计算第一个快，所以它通过fs.getFileStatus 和 fs.getFileBlockLocations 拿到需要计算的那个块
		// 这叫数据本地化读取，计算向数据移动之后期望的是分治计算，只读取自己关心的（通过seek实现），同时具备距离的概念，优先和本地的DN获取数据
		// -- 框架的默认机制
		in.seek(1048576);
		System.out.println((char)in.readByte()); //每次读一个字符
		System.out.println((char)in.readByte());
		System.out.println((char)in.readByte());
		System.out.println((char)in.readByte());
		System.out.println((char)in.readByte());
		System.out.println((char)in.readByte());
		System.out.println((char)in.readByte());
		System.out.println((char)in.readByte());
		System.out.println((char)in.readByte());
		System.out.println((char)in.readByte());
		System.out.println((char)in.readByte());
		System.out.println((char)in.readByte());
		System.out.println((char)in.readByte());
	}

	@After
	public void close() throws IOException {
		fs.close();
	}
}
