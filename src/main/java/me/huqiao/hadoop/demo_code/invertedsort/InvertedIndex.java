package me.huqiao.hadoop.demo_code.invertedsort;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import me.huqiao.hadoop.demo_code.accesslog.responsetime.sort.URLResponseTime;
import me.huqiao.hadoop.demo_code.accesslog.responsetime.sort.URLResponseTimeSortMapper;
import me.huqiao.hadoop.demo_code.accesslog.responsetime.sort.URLResponseTimeSortReducer;
import me.huqiao.hadoop.demo_code.accesslog.responsetime.sort.URLResponseTimeSortStarter;

/**
 * 
 *
 * <pre>
 *file1.txt:
 *hello ketty
 *hello tomcat
 *
 *file2.txt:
 *hello hadoop
 *
 *map1:
 *hello:file1.txt 1
 *hello:file1.txt 1
 *ketty:file1.txt 1
 *tomcat:file1.txt 1
 *hello:file2.txt 1
 *hadoop:file2.txt 1
 *
 *reduce1:
 *hello:file1.txt 2
 *ketty:file1.txt 1
 *tomcat:file1.txt 1
 *hello:file2.txt 1
 *hadoop:file2.txt 1
 *
 *reduce2:
 *hello file1.txt 2,file2.txt 1
 *ketty file1.txt 1
 *tomcat file1.txt 1
 *hadoop file2.txt 1
 *</pre>
 * @author huqiao
 */
public class InvertedIndex {
	
	/**
	 * input:files to be inverted index<br/>
	 * output: someword:filename  count
	 * @author huqiao
	 */
	static class WordInFileCountMapper extends Mapper<LongWritable,Text,Text,LongWritable>{

		@Override
		protected void map(LongWritable key, Text value,Context ctx)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] words = line.split(" ");
			
			FileSplit fileSplit = (FileSplit)ctx.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			for(String word : words) {
				ctx.write(new Text(word + ":" + fileName), new LongWritable(1));
			}
		}
		
	}
	
	/**
	 * output:
	 * <pre>
	 *hello:file1.txt 2
	 *ketty:file1.txt 1
	 *tomcat:file1.txt 1
	 *hello:file2.txt 1
	 *hadoop:file2.txt 1
	 *</pre>
	 * @author huqiao
	 */
	static class WordInFileCountReducer extends Reducer<Text,LongWritable,Text,LongWritable>{

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context ctx) throws IOException, InterruptedException {
			int total = 0;
			for(LongWritable value : values) {
				total += value.get();
			}
			ctx.write(key, new LongWritable(total));
		}
		
	}
	
	
	/**
	 * output:
	 * <pre>
	 * hello-->WordCountRecord{fileName:file1.txt,count:2}
	 * ...
	 * </pre>
	 * @author huqiao
	 */
	static class InvertedIndexMapper extends Mapper<LongWritable,Text,Text,WordCountRecord>{

		@Override
		protected void map(LongWritable key, Text value,Context ctx)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] lineArray = line.split("\t");
			String[] wordAndFileName = lineArray[0].split(":");
			String word = wordAndFileName[0];
			String fileName = wordAndFileName[1];
			Long count = Long.parseLong(lineArray[1]);
			
			ctx.write(new Text(word), new WordCountRecord(fileName, count));
			
		}
		
	}
	
	/**
	 * output:
	 * <pre>
	 * hello-->file1.txt 2,file2.txt 1
	 * ...
	 * </pre>
	 * @author huqiao
	 */
	static class InvertedIndexReducer extends Reducer<Text,WordCountRecord,Text,Text>{

		@Override
		protected void reduce(Text key, Iterable<WordCountRecord> values, Context ctx) throws IOException, InterruptedException {
			 StringBuffer output = new StringBuffer();
			 for(WordCountRecord value : values) {
				 output.append(value.getFileName() + " " + value.getCount()+",");
			 }
			 ctx.write(key, new Text(output.toString()));
		}
		
	}
	
	public static void main(String[] args) throws Exception{
		
		String inputPath = args[0];
        String outputPath = args[1];
        String phase = args[2];
        
        FileSystem fs = FileSystem.get(new URI("hdfs://vcentos1:9000"),new Configuration(),"root");
        
        //delete output path when it existed
        Path output = new Path(outputPath);
        if(fs.exists(output)) {
        	fs.delete(output,true);
        }
        
        if("phase1".equals(phase)) {
        	 doPhase1(inputPath,outputPath);
        }else {
        	doPhase2(inputPath,outputPath);
        }
       
		
	}

	private static void doPhase1(String inputPath,String outputPath)throws Exception {
		 	Job job = Job.getInstance();
	        
	        job.setJarByClass(InvertedIndex.class);
	        
	        job.setMapperClass(WordInFileCountMapper.class);
	        job.setReducerClass(WordInFileCountReducer.class);
	        
	        job.setMapOutputKeyClass(Text.class);
	        job.setMapOutputValueClass(LongWritable.class);
	        
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(LongWritable.class);
	        
	        FileInputFormat.setInputPaths(job, new Path(inputPath));
	        FileOutputFormat.setOutputPath(job, new Path(outputPath));
	        
	        boolean success = job.waitForCompletion(true);
	        
	        System.exit(success ? 0 : 1);
	}
	
	private static void doPhase2(String inputPath,String outputPath)throws Exception {
		Job job = Job.getInstance();
		
		job.setJarByClass(InvertedIndex.class);
		
		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(WordCountRecord.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		boolean success = job.waitForCompletion(true);
		
		System.exit(success ? 0 : 1);
	}

}
