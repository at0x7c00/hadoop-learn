package me.huqiao.hadoop.demo_code.invertedsort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * record total count of a word in a file
 * @author huqiao
 */
public class WordCountRecord implements WritableComparable<WordCountRecord>{

	String fileName;
	Long count;
	
	
	
	public WordCountRecord() {
	}
	public WordCountRecord(String fileName, Long count) {
		this.fileName = fileName;
		this.count = count;
	}
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public Long getCount() {
		return count;
	}
	public void setCount(Long count) {
		this.count = count;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(fileName);
		out.writeLong(count);
	}
	public void readFields(DataInput in) throws IOException {
		this.fileName = in.readUTF();
		this.count = in.readLong();
	}
	
	public int compareTo(WordCountRecord wcr) {
		return -this.count.compareTo(wcr.count);
	}
}
