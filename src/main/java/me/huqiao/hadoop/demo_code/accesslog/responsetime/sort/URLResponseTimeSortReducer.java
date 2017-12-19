package me.huqiao.hadoop.demo_code.accesslog.responsetime.sort;

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
public class URLResponseTimeSortReducer extends Reducer<URLResponseTime, LongWritable, URLResponseTime, LongWritable> {

	
	@Override
	protected void reduce(URLResponseTime key, Iterable<LongWritable> values,
			Context ctx) throws IOException, InterruptedException {
		ctx.write(key, values.iterator().next());
	}

	
}
