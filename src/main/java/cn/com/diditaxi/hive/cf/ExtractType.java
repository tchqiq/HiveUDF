package cn.com.diditaxi.hive.cf;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

/**
 * 2015-02-05 heqi
 */
@Description(name = "extract_type", value = "_FUNC_(str,'ab.properties') "
		+ "从配置文件读取，返回匹配", extended = "Example:\n"
		+ "  > SELECT extract_type_from_curl(curl) FROM file_pv_track a;\n")
public final class ExtractType extends UDF {

	private static Map<String, Map<String, String>> propMap = Maps.newHashMap();
	private static Properties p = new Properties();
	private static Map<String, String> m = Maps.newHashMapWithExpectedSize(150);
	private Text output = new Text();

	public static void main(String[] args) {
		ExtractType ep = new ExtractType();
		Text t = new Text();
		Text tt = new Text();
		Text t2 = new Text();
		Text tt2 = new Text();
		t.set("12");
		tt.set("city.properties");
		t2.set("11");
		tt2.set("city.properties");
		System.out.println(ep.evaluate(t, tt).toString());

	}
	

	public Text evaluate(Text s, Text prop) {

		output.set("-");

		if (s == null) {
			return output;
		}

		loadMap(prop.toString());
		String type = extractPid(s.toString(), prop.toString());

		if (Strings.isNullOrEmpty(type)) {
			return output;
		}

		output.set(type);
		return output;
	}

	private static void loadMap(String prop) {
		if (!propMap.containsKey(prop)) {
			if (m.size() > 0)
				m = Maps.newHashMap();
			try {
				String path = "/" + prop;
				p.load(ExtractType.class.getResourceAsStream(path));
				m = Maps.fromProperties(p);

			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}

			propMap.put(prop, m);

		}
	}

	private String extractPid(String str, String prop) {

		Map<String, String> ruleMap = propMap.get(prop);
		// 配置文件中指定了如果为没有匹配到规则时的显示值
		String type = "-";

		if (Strings.isNullOrEmpty(str))
			return type;

		if (ruleMap.containsKey(str)) {
			type = ruleMap.get(str);
		}

		return type;
	}

}