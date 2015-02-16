package cn.com.diditaxi.hive.cf;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

/**
 * 2014-06-20 heqi
 */
@Description(name = "expid", value = "_FUNC_(str) - 从curl中抽取预定义的产品类型", extended = "Example:\n"
		+ "  > SELECT extract_pid_from_curl(curl) FROM file_pv_track a;\n")
public final class ExtractPid extends UDF {


	private static final String LogDataException = "LogDataException";
	private static Map<String, Map<String, String>> propMap = Maps.newHashMap();
	private static Map<String, Map<String, String>> ppMap = Maps.newHashMap();
	private static Properties p = new Properties();
	private static Map<String, String> m = Maps.newHashMapWithExpectedSize(150);
	private static TreeMap<String, String> pathRuleTree = Maps
			.newTreeMap(new Comparator<String>() {

				@Override
				public int compare(final String o1, final String o2) {

					return -(o1.length() - o2.length());
				}
			});
	private Text output = new Text();

	public static void main(String[] args) {
		ExtractPid ep = new ExtractPid();
		Text t = new Text();
		Text tt = new Text();
		t.set("http://blog.csdn.net/xuexiaodong009/article/details/18301645");
		tt.set("pty2.properties");
		System.out.println(ep.evaluate(t, tt).toString());

		t.set("http://club.csdn.net/community");
		tt.set("pid2.properties");
		System.out.println(ep.evaluate(t, tt).toString());

		t.set("http://bbs.csdn.net/topics/310064554");
		tt.set("pty2.properties");
		System.out.println(ep.evaluate(t, tt).toString());

		t.set("http://club.csdn.net/cmdn/");
		tt.set("pid2.properties");
		System.out.println(ep.evaluate(t, tt).toString());

	}

	public Text evaluate(Text s, Text prop) {

		output.set(LogDataException);

		if (s == null) {
			return output;
		}

		loadMap(prop.toString());
		String pid = extractPid(s.toString(), prop.toString());

		if (Strings.isNullOrEmpty(pid)) {
			return output;
		}

		output.set(pid);
		return output;
	}

	private void loadMap(String prop) {
		if (!propMap.containsKey(prop)) {
			if (m.size() > 0)
				m = null;
			try {
				String path = "/" + prop;
				p.load(ExtractPid.class.getResourceAsStream(path));
				m = Maps.fromProperties(p);

			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}

			if ("url".equals(m.get("type"))) {
				if (pathRuleTree.size() > 0)
					pathRuleTree.clear();
				Iterator<String> rkIter = m.keySet().iterator();

				while (rkIter.hasNext()) {
					String key = rkIter.next();

					if (key.indexOf("/") > 0) {
						pathRuleTree.put(key, m.get(key));
					}
				}
				ppMap.put(prop, pathRuleTree);

			}
			p.clear();
			propMap.put(prop, m);

		}
	}

	private String extractPid(String str, String prop) {

		Map<String, String> ruleMap = propMap.get(prop);
		// 配置文件中指定了如果为没有匹配到规则时的显示值
		String pid = ruleMap.get("N");

		if (Strings.isNullOrEmpty(str))
			return LogDataException;

		// 如果是url
		if ("url".equals(ruleMap.get("type"))) {
			if (str.startsWith("http://") || str.startsWith("https://")) {
				pid = processUrl(str, ruleMap, ppMap.get(prop));
			} else {
				if ("-".equals(str)) {
					pid = ruleMap.get("-");
				} else {
					pid = LogDataException;
				}
			}
		} else {
			if (ruleMap.containsKey(str)) {
				pid = ruleMap.get(str);
			}
		}

		return pid;
	}

	private String processUrl(String str, Map<String, String> ruleMap,
			Map<String, String> treeMap) {

		try {
			String host = new URL(str).getHost();

			if (host.contains(".iteye.")) {
				return ruleMap.get("www.iteye.com");
			}

			for (String tKey : treeMap.keySet()) {
				if (str.startsWith("http://" + tKey)
						|| str.startsWith("https://" + tKey)) {
					return treeMap.get(tKey);
				}
			}

			return ruleMap.get(host);

		} catch (MalformedURLException e) {
			e.printStackTrace();
		}

		return null;
	}

}