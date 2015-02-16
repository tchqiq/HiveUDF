package cn.com.diditaxi.hive.cf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * heqi 2014-05-30
 */
@Description(name = "rank", 
			value = "_FUNC_(key) - 按行简单的计数，遇到不同值则置0",
			extended = "Example:\n"
		+ "  > SELECT rank(key) FROM file_pv_track a;\n")

/**
 * 
 * The rank function keeps track of last user key 
 * and simply increments the counter. 
 * As soon as it sees a new user, it reset counter to zero
 *
 */
public final class Rank extends UDF {
	private static int counter;
	private String last_key;

	public int evaluate(final String key) {
		if (!key.equalsIgnoreCase(this.last_key)) {
			this.counter = 0;
			this.last_key = key;
		}
		return this.counter++;
	}
}
