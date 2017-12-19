package me.huqiao.hadoop.demo_code.accesslog.responsetime.sort;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class URLResponseTimeSortStarter {

	public static void main(String[] args)throws Exception {
		String inputPath = args[0];
        String outputPath = args[1];
        
        FileSystem fs = FileSystem.get(new URI("hdfs://centos01:9000"),new Configuration(),"root");
        
        //delete output path when it existed
        Path output = new Path(outputPath);
        if(fs.exists(output)) {
        	fs.delete(output,true);
        }
        
        Job job = Job.getInstance();
        
        job.setJarByClass(URLResponseTimeSortStarter.class);
        
        job.setMapperClass(URLResponseTimeSortMapper.class);
        job.setReducerClass(URLResponseTimeSortReducer.class);
        
        job.setMapOutputKeyClass(URLResponseTime.class);
        job.setMapOutputValueClass(LongWritable.class);
        
        job.setOutputKeyClass(URLResponseTime.class);
        job.setOutputValueClass(LongWritable.class);
        
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        boolean success = job.waitForCompletion(true);
        
        System.exit(success ? 0 : 1);
	}
	
}
