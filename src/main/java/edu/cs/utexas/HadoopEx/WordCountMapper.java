package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

	// Create a counter and initialize with 1
	private final IntWritable counter = new IntWritable(1);
	// Create a hadoop text object to store words
	private Text word = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

		String line = value.toString();
		
		StringTokenizer itr = new StringTokenizer(line, ",");
		for(int i = 0; i < 7 && itr.hasMoreTokens(); i++){
			itr.nextToken();
		}

		word.set(itr.nextToken());
		context.write(word, counter);

	}
}