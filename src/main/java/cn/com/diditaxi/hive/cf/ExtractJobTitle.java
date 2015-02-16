package cn.com.diditaxi.hive.cf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 9/14/13 heqi
 */
@Description(name = "reg_match",
	value = "_FUNC_(str) - 正则轮流匹配"
	, extended = "Example:\n"
				 + "  > SELECT _FUNC_(tag,rule) FROM file_pv_track;\n")
public final class ExtractJobTitle extends UDF {

	public static void main(String[] args) {

		ExtractJobTitle rmi = new ExtractJobTitle();
		Text t = new Text("IOS高级开发工程师");
		Text tt = new Text("jobtitle.properties");
		System.out.println(rmi.evaluate(t, tt));

	}

	private static TreeMap<String, String> treeMap = new TreeMap(
			new Comparator<String>() {

				@Override
				public int compare(final String o1, final String o2) {
					int l1 = o1.length();
					int l2 = o2.length();
					if(l1==l2) {
						return o1.compareTo(o2);
					}
					return -(o1.length() - o2.length());
				}
			});
	
	private Text output = new Text();

	public Text evaluate(Text s, Text rule) {
		if (s == null) {
			output.set("-");
			return output;
		}
		output = matchInturn(s.toString().toLowerCase(), rule.toString());
		return output;
	}

	private Text matchInturn(String str, String rulePath) {
		Properties prop = new Properties();
		output.set("other");
		
		try {
			prop.load(ExtractJobTitle.class.getResourceAsStream("/" + rulePath));
			for(String key : prop.stringPropertyNames()) {
				treeMap.put(key, prop.getProperty(key));
			}
			
			for(String regexp : treeMap.keySet()) {
				Pattern p = Pattern.compile(regexp);
				Matcher m = p.matcher(str);
				if(m.find()) {

					output.set(prop.getProperty(regexp));
					return output; 
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return output;
	}

}
