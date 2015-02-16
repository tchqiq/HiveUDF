package cn.com.diditaxi.hive.cf;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * 2014/09/26
 * @author heqi
 *
 */
@Description(name = "thread", 
			 value = "_FUNC_(skill,score) -根据技能分数返回是否高于阈值。0：小于，1：大于等于 ",
			 extended = "Example:\n"
		+ "  > SELECT thread(skill,score) FROM file_pv_track a;\n")
public final class OverThreshold extends UDF {

	public static void main(String[] args) {
		OverThreshold ep = new OverThreshold();
		Text t = new Text();
		Text tt = new Text();
		t.set("abc");
		tt.set("0.022163121");
		System.out.println(ep.evaluate(t, tt));
	}

	static Properties p = new Properties();
	
	static {
		try {
			p.load(OverThreshold.class.getResourceAsStream("/" + "thread.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	IntWritable flag = new IntWritable(-1);
	
	public IntWritable evaluate(Text skill, Text score) {

		if (skill == null || score == null) {
			return flag;
		}
		
		double thread = 0.0; 
		if(null != String.valueOf(p.get(skill.toString())) && !"null".equals(String.valueOf(p.get(skill.toString())))) {
			thread=	Double.parseDouble(String.valueOf(p.get(skill.toString())));
		}
		
		if (Double.parseDouble(score.toString()) >= thread) {
			flag.set(1);
		} else {
			flag.set(0);
		}

		return flag;
	}

}