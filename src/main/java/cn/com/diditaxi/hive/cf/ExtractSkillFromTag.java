package cn.com.diditaxi.hive.cf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 9/14/13 heqi
 */
@Description(name = "get_skill",
	value = "_FUNC_(tag) - 正则匹配tag，抽取技能"
	, extended = "Example:\n"
				 + "  > SELECT _FUNC_(tag,rule) FROM file_pv_track;\n")
public final class ExtractSkillFromTag extends UDF {

	public static void main(String[] args) {

//		ExtractSkillFromTag rmi = new ExtractSkillFromTag();
//		Text t = new Text("c");
//		System.out.println(rmi.evaluate(t));

	}
	
	private static Properties prop = new Properties();
	private static Map<String, String> output = new HashMap<String, String>(2);
	
	static {
		try {
			prop.load(ExtractSkillFromTag.class.getResourceAsStream("/tag4skill.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	public Map<String, String> evaluate(Text s) {
		if (s == null) {
			return output;
		}
		output = matchInturn(s.toString());
		return output;
	}

	private Map<String, String> matchInturn(String tag) {
		
		for(String regexp : prop.stringPropertyNames()) {
			Pattern p = Pattern.compile(regexp);
			Matcher m = p.matcher(tag.toLowerCase());

			if(m.find()) {
				output.put("skill", prop.getProperty(regexp));
				output.put("mtag", m.group());
			}
			
		}
		
		return output;
	}

}
