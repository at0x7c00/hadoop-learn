package me.huqiao.hadoop.demo_code.accesslog.responsetime;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.WritableComparable;

public class MyText implements WritableComparable<MyText>{
	String value;
	int age;
	Random random = new Random();

	public void write(DataOutput out) throws IOException {
		out.writeUTF(value);
		out.writeInt(age);
	}

	public void readFields(DataInput in) throws IOException {
		this.value = in.readUTF();
		this.age = random.nextInt(100);
	}

	public int compareTo(MyText o) {
		return this.value.compareTo(o.value);
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public MyText(String value) {
		this.value = value;
		this.age = random.nextInt();
	}

	public MyText() {
	}

	@Override
	public String toString() {
		return this.age + "," + value;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}
	

}
