package me.huqiao.hadoop.demo_code.accesslog.responsetime;

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
 * 111.200.202.123 - - [19/Dec/2017:10:05:47 +0800] "GET /assets/global/plugins/bootstrap/css/bootstrap.min.css HTTP/1.1" 200 124473
 * 111.200.202.123 - - [19/Dec/2017:10:05:47 +0800] "GET /assets/global/plugins/font-awesome/css/font-awesome.min.css HTTP/1.1" 200 20766
 * 111.200.202.123 - - [19/Dec/2017:10:05:47 +0800] "GET /assets/global/css/smartadmin-production-plugins.min.css HTTP/1.1" 200 158217
 * </pre>
 * @author huqiao
 */
public class URLResponseTimeMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
	

	//make a member property to avoid new instance every time when map function invoked.
	Text key = new Text();
	LongWritable value = new LongWritable();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] logs = line.split(" ");
		if(logs.length<10) {
			return;
		}
		String path = logs[6];
		String responseTimeStr = logs[9];
		
		//ignore row whitch has no response time
		if(responseTimeStr.trim().equals("-")) {
			return;
		}
		
		if(path.trim().equals("/")) {
			return;
		}
		
		if(path.indexOf("?")>0) {
			path = path.substring(0, path.indexOf("?"));
		}
			
		
		long responseTime = Long.parseLong(responseTimeStr);
		
		
		this.key.set(path);
		this.value.set(responseTime);
		context.write(this.key,this.value);
	}

	
}
