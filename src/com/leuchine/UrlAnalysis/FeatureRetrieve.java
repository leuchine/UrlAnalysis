package com.leuchine.UrlAnalysis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Iterator;
import java.util.LinkedList;

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

public class FeatureRetrieve {
	public static int featureNumber = 8;
	public static int isIPAddress = 0;
	public static int isContainNumber = 1;
	public static int checkSimilarity = 2;
	public static int containKey = 3;
	public static int containBank = 4;
	public static int checkTopDomain = 5;
	public static int checkHierarchy = 6;
	public static int searchNumber = 7;

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] lineSplit = line.split(" +");
			if (lineSplit.length == 2
					&& (lineSplit[0].length() > 3 && lineSplit[1].trim()
							.length() == 1) && !lineSplit[1].equals("")) {
				if (lineSplit[1].equals("0"))
					output.collect(new Text(lineSplit[0]), new IntWritable(0));
				else
					output.collect(new Text(lineSplit[0]), new IntWritable(1));
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, NullWritable> {

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, NullWritable> output, Reporter reporter)
				throws IOException {
			LinkedList<String> urlSplit = splitURL(key.toString());
			boolean[] features = analyzeURL(urlSplit, key.toString());
			StringBuilder out = new StringBuilder();
			for (int i = 0; i < featureNumber; i++) {
				if (features[i] == true)
					out.append("1,");
				else
					out.append("0,");
			}
			if (values.next().get() == 0)
				out.append("0");
			else
				out.append("1");
			output.collect(new Text(out.toString()), NullWritable.get());
		}
	}

	public static boolean[] analyzeURL(LinkedList<String> splits,
			String completeURL) {
		boolean features[] = new boolean[featureNumber];
		String url = splits.get(0);
		String path = splits.get(1);
		features[isIPAddress] = checkIPAddress(url);
		features[isContainNumber] = checkContainNumber(url);
		features[checkSimilarity] = checkSimilarity(url);
		features[containKey] = containKey(url, path);
		features[containBank] = containBank(url, path);
		features[checkTopDomain] = checkTopDomain(url);
		features[checkHierarchy] = checkHierarchy(url);
		features[searchNumber] = checkSearchNumber(url);
		return features;
	}

	public static boolean checkHierarchy(String url) {
		String[] urlSplit = url.split("\\.");
		if (urlSplit.length > 4)
			return true;
		return false;
	}

	public static boolean checkTopDomain(String url) {
		String[] urlSplit = url.split("\\.");
		if (!urlSplit[urlSplit.length - 1].equals("cn")
				&& !urlSplit[urlSplit.length - 1].equals("com")) {
			return true;
		}
		return false;
	}

	public static boolean containKey(String url, String path) {
		if (url.contains("hunantv") || url.contains("boc")
				|| url.contains("taobao") || url.contains("icbc")
				|| url.contains("alipay") || url.contains("paypal")
				|| path.contains("hunantv") || path.contains("boc")
				|| path.contains("taobao") || path.contains("icbc")
				|| path.contains("alipay") || path.contains("paypal"))
			return true;
		return false;
	}

	public static boolean containBank(String url, String path) {
		if (url.contains("account") || url.contains("login")
				|| path.contains("account") || path.contains("login"))
			return true;
		return false;
	}

	public static boolean checkSimilarity(String url) {
		String[] urlSplit = url.split("\\.");
		String domain = null;
		if (!urlSplit[urlSplit.length - 1].equals("cn")) {
			try {
				domain = urlSplit[urlSplit.length - 2];
			} catch (Exception e) {
				domain = urlSplit[urlSplit.length - 1];
			}

		} else {
			try {
				domain = urlSplit[urlSplit.length - 3];
			} catch (Exception e) {
				domain = urlSplit[urlSplit.length - 2];
			}
		}
		int sim1 = stringSimilarity(domain, "hunantv");
		int sim2 = stringSimilarity(domain, "taobao");
		int sim3 = stringSimilarity(domain, "icbc");
		int sim4 = stringSimilarity(domain, "alipay");
		int sim5 = stringSimilarity(domain, "paypal");
		if (sim1 <= 3 || sim2 <= 3 || sim3 <= 3 || sim4 <= 3 || sim5 <= 3)
			return true;
		return false;
	}

	public static boolean checkSearchNumber(String domain) {
		URL url = null;
		URLConnection con = null;

		try {
			url = new URL("https://www.google.com.hk/search?q=" + domain);
			System.out.println("Check search result " + domain);
			System.out.println("--------");
			con = url.openConnection();
			con.setRequestProperty(
					"User-Agent",
					"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.95 Safari/537.36");
			con.setRequestProperty("Accept", "*/*");
			con.setRequestProperty("Accept-Language", "en;q=0.8");
			con.setRequestProperty("Connection", "keep-alive");
			BufferedReader scan = new BufferedReader(new InputStreamReader(
					con.getInputStream(), "utf-8"));
			String line = scan.readLine();
			while (line != null) {
				if (line.contains("div id=\"resultStats\"")) {
					int index = line.indexOf("div id=\"resultStats\"");
					line = line.substring(index + 20);
					index = line.indexOf("<nobr>");
					line = line.substring(0, index);
					StringBuilder sb = new StringBuilder(line);
					for (int i = 0; i < sb.length(); i++) {
						if (sb.charAt(i) != '0' && sb.charAt(i) != '1'
								&& sb.charAt(i) != '2' && sb.charAt(i) != '3'
								&& sb.charAt(i) != '4' && sb.charAt(i) != '5'
								&& sb.charAt(i) != '6' && sb.charAt(i) != '7'
								&& sb.charAt(i) != '8' && sb.charAt(i) != '9') {
							sb.setCharAt(i, ' ');
						}
					}
					line = sb.toString().trim();
					line = line.replace(",", "");
					line = line.replace(" ", "");
					long searchNumber = 6000;
					try {
						searchNumber = Long.parseLong(line.trim());
					} catch (Exception e) {

					}
					System.out.println(searchNumber);
					if (searchNumber > 5000)
						return false;
					else
						return true;
				}
				line = scan.readLine();
			}
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}

	public static boolean checkContainNumber(String url) {
		if (url.contains("0") || url.contains("1") || url.contains("2")
				|| url.contains("3") || url.contains("4") || url.contains("5")
				|| url.contains("6") || url.contains("7") || url.contains("8")
				|| url.contains("9"))
			return true;
		return false;
	}

	public static boolean checkIPAddress(String url) {
		String[] urlSplit = url.split("\\.");
		boolean result = true;
		for (int i = 0; i < urlSplit.length; i++) {
			boolean isAllDigit = false;
			try {
				Integer.parseInt(urlSplit[i]);
				isAllDigit = true;
			} catch (Exception ee) {

			}
			if (isAllDigit == false) {
				result = false;
				break;
			}
		}
		return result;
	}

	public static LinkedList<String> splitURL(String url) {
		LinkedList<String> splits = new LinkedList<String>();
		if (url.contains("://")) {
			int start = url.indexOf("://");
			url = url.substring(start + 3);
		}
		if (url.contains("/")) {
			int start = url.indexOf("/");
			String path = url.substring(start);
			url = url.substring(0, start);
			splits.addFirst(path);
		} else {
			splits.addFirst(new String(""));
		}
		splits.addFirst(url);
		return splits;

	}

	public static int stringSimilarity(String strA, String strB) {
		int lenA = strA.length();
		int lenB = strB.length();
		int[][] simTable = new int[lenA + 1][lenB + 1];
		for (int i = 0; i <= lenB; i++)
			simTable[lenA][i] = lenB - i;
		for (int i = 0; i <= lenA; i++)
			simTable[i][lenB] = lenA - i;
		for (int i = lenA - 1; i >= 0; i--)
			for (int j = lenB - 1; j >= 0; j--) {
				if (strA.charAt(i) == strB.charAt(j))
					simTable[i][j] = simTable[i + 1][j + 1];
				else
					simTable[i][j] = min(simTable[i + 1][j + 1],
							simTable[i][j + 1], simTable[i + 1][j]) + 1;
			}
		return simTable[0][0];
	}

	public static int min(int one, int two, int three) {
		int min = one;
		if (two < min) {
			min = two;
		}
		if (three < min) {
			min = three;
		}
		return min;
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(FeatureRetrieve.class);
		conf.setJobName("FeatureRetrieve");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setNumReduceTasks(4);

		FileInputFormat.setInputPaths(conf, new Path("/input2.txt"));
		FileOutputFormat.setOutputPath(conf, new Path("/output3"));

		JobClient.runJob(conf);
	}
}
