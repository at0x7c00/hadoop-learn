package me.huqiao.hadoop.demo_code.accesslog.responsetime.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class URLResponseTime implements WritableComparable<URLResponseTime>{
	
	String url;
	long avgResponseTime;

	public void write(DataOutput out) throws IOException {
		out.writeUTF(url);
		out.writeLong(avgResponseTime);
	}

	public void readFields(DataInput in) throws IOException {
		this.url = in.readUTF();
		this.avgResponseTime = in.readLong();
	}

	public int compareTo(URLResponseTime urt) {
		return this.avgResponseTime > urt.avgResponseTime ? -1 : 1;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public long getAvgResponseTime() {
		return avgResponseTime;
	}

	public void setAvgResponseTime(long avgResponseTime) {
		this.avgResponseTime = avgResponseTime;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((url == null) ? 0 : url.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		URLResponseTime other = (URLResponseTime) obj;
		if (url == null) {
			if (other.url != null)
				return false;
		} else if (!url.equals(other.url))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return url;
	}
	
}
