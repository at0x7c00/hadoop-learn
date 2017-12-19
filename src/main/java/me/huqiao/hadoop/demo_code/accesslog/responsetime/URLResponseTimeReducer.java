package me.huqiao.hadoop.demo_code.accesslog.responsetime;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * KIN:access path
 * VIN:response times
 * KOUT:access path
 * VOUT:average response time
 * eg.
 * <pre>
 * /assets/global/plugins/bootstrap/css/bootstrap.min.css 124473
 * /assets/global/plugins/font-awesome/css/font-awesome.min.css 20766
 * /assets/global/css/smartadmin-production-plugins.min.css HTTP/1.1 158217
 * </pre>
 * @author huqiao
 */
public class URLResponseTimeReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	LongWritable value = new LongWritable();
	
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Context ctx) throws IOException, InterruptedException {
		
		int count = 0;
		long totalResponseTime = 0;
		for(LongWritable value : values) {
			totalResponseTime += value.get();
			count++;
		}
		long averageResponseTime = 0;
		if(count!=0) {
			averageResponseTime = totalResponseTime / count;
		}
		
		this.value.set(averageResponseTime);
		ctx.write(key, this.value);
		
		
	}

	
}
