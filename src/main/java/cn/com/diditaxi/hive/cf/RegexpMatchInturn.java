package cn.com.diditaxi.hive.cf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 9/14/13 heqi
 */
@Description(name = "reg_match",
	value = "_FUNC_(str) - 正则轮流匹配"
	, extended = "Example:\n"
				 + "  > SELECT _FUNC_(tag,rule) FROM file_pv_track;\n")
public final class RegexpMatchInturn extends UDF {

	public static void main(String[] args) {

		RegexpMatchInturn rmi = new RegexpMatchInturn();
		Text t = new Text("mongodb");
		Text tt = new Text("jobtitle.properties");
		System.out.println(rmi.evaluate(t, tt));

	}

	private Map<String, String> output = Maps.newHashMap();

	public Map<String, String> evaluate(Text s, Text rule) {
		output = null;
		if (s == null) {
			return output;
		}
		output = matchInturn(s.toString(), rule.toString());
		return output;
	}

	private Map<String, String> matchInturn(String str, String rulePath) {
		
		Properties prop = new Properties();
		
		output = Maps.newHashMap();
		List<String> tags = Lists.newArrayList();
		List<String> keys = Lists.newArrayList();
		
		try {
			prop.load(RegexpMatchInturn.class.getResourceAsStream("/" + rulePath));
			
			for(String regexp : prop.stringPropertyNames()) {
				Pattern p = Pattern.compile(regexp);
				Matcher m = p.matcher(str);
				
				if(m.find()) {
					tags.add(m.group());
					keys.add(prop.getProperty(regexp));

					while (m.find()) {
						tags.add(m.group());
					}

					output.put("key", Joiner.on("|").join(keys));
					output.put("tag", Joiner.on("|").join(tags));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return output;
	}

}
