package com.leuchine.UrlAnalysis;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

public class URLPattern {
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, Text> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			Pattern[] patterns = new Pattern[4];
			patterns[0] = Pattern
					.compile("(http|www|ftp)?(://)?[^\\s]*\\.(com|cn|net|org|biz|info|cc|tv|edu|hk)*/");
			patterns[1] = Pattern
					.compile("(http|www|ftp)?(://)?[^\\s]*\\.(com|cn|net|org|biz|info|cc|tv|edu|hk)*/[^\\s]*/");
			patterns[2] = Pattern
					.compile("(http|www|ftp)?(://)?[^\\s]*\\.(com|cn|net|org|biz|info|cc|tv|edu|hk)*/[^\\s]*");
			patterns[3] = Pattern
					.compile("(http|www|ftp)?(://)?(sports){1}\\.[^\\s]*\\.(com|cn|net|org|biz|info|cc|tv|edu|hk)*/([^\\s]*)?");
			for (int i = 0; i < 4; i++) {
				if (patterns[i].matcher(value.toString()).matches()) {
					output.collect(new IntWritable(i), value);
				}
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<IntWritable, Text, Text, NullWritable> {
		MultipleOutputs mo = null;
		@Override
		public void configure(JobConf job) {
			mo = new MultipleOutputs(job);
		}

		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<Text, NullWritable> output, Reporter reporter)
				throws IOException {
			int fileNumber = key.get() + 1;
			while (values.hasNext()) {
				mo.getCollector("URL" + fileNumber, reporter).collect(
						values.next(), NullWritable.get());
			}

		}

		public void close() {
			try {
				mo.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		FileSystem hdfs = FileSystem.get(configuration);

		FileStatus[] status = hdfs.listStatus(new Path("/input"));
		Path[] paths = new Path[status.length];
		for (int i = 0; i < status.length; i++) {
			paths[i] = status[i].getPath();
		}

		JobConf conf = new JobConf(URLPattern.class);
		conf.setJobName("urlpattern");

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setNumReduceTasks(4);

		FileInputFormat.setInputPaths(conf, paths);
		FileOutputFormat.setOutputPath(conf, new Path("/output2"));

		MultipleOutputs.addNamedOutput(conf, "URL1", TextOutputFormat.class,
				Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(conf, "URL2", TextOutputFormat.class,
				Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(conf, "URL3", TextOutputFormat.class,
				Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(conf, "URL4", TextOutputFormat.class,
				Text.class, NullWritable.class);
		JobClient.runJob(conf);
	}
}
