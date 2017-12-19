package me.huqiao.hadoop.demo_code.accesslog.responsetime.sort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * KIN:log row number
 * VIN:log row content
 * KOUT:access path
 * VOUT:response time
 * eg.
 * <pre>
 * /assets/global/plugins/bootstrap/css/bootstrap.min.css 124473
 * /assets/global/plugins/font-awesome/css/font-awesome.min.css 20766
 * </pre>
 * @author huqiao
 */
public class URLResponseTimeSortMapper extends Mapper<LongWritable,Text,URLResponseTime,LongWritable>{
	

	//make a member property to avoid new instance every time when map function invoked.
	URLResponseTime key = new URLResponseTime();
	LongWritable value = new LongWritable();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] logs = line.split("\t");
		String url = logs[0];
		String responseTimeStr = logs[1];
		
		long responseTime = Long.parseLong(responseTimeStr);
		
		
		this.key.setUrl(url);
		this.key.setAvgResponseTime(responseTime);
		this.value.set(responseTime);
		context.write(this.key,this.value);
	}

	
}
