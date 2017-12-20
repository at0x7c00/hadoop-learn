package me.huqiao.hadoop.demo_code.accesslog.responsetime;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class URLResponseTimePartitioner extends Partitioner<MyText, LongWritable>{

	@Override
	public int getPartition(MyText key, LongWritable value, int numPartitions) {
		String accessPath = key.getValue();
		if(accessPath.endsWith(".do")) {
			return 0;
		}
		return 1;
	}
	
}
