package me.huqiao.hadoop.demo_code.accesslog.responsetime;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class URLResponseTimePartitioner extends Partitioner<Text, LongWritable>{

	@Override
	public int getPartition(Text key, LongWritable value, int numPartitions) {
		String accessPath = key.toString();
		if(accessPath.endsWith(".do")) {
			return 0;
		}
		return 1;
	}
	
}
