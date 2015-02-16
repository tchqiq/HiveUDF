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
public final class ExtractCMS extends UDF {


	private static final String LogDataException = "-";
	private Text output = new Text();

	public static void main(String[] args) {
		ExtractCMS ep = new ExtractCMS();
		Text t = new Text();
		Text tt = new Text();
		t.set("http://it.sohu.com/20141223/n407197923.shtml?ref=myread");
		
		System.out.println(ep.evaluate(t, tt).toString());

	}

	public Text evaluate(Text s, Text prop) {

		output.set(LogDataException);

		if (s == null) {
			return output;
		}
		String str = s.toString();
		
		if (str.startsWith("http://") || str.startsWith("https://")) {
			String pid = processUrl(str);
			if(Strings.isNullOrEmpty(pid)) {
				pid = "geek";
			}
			output.set(pid);
		} 
		
		return output;
	}

	static Map<String,String> ruleMap = new HashMap<String,String>(25);
	static {
		ruleMap.put("tech.qq.com", "cms");
		ruleMap.put("tech.163.com", "cms");
		ruleMap.put("tech.sina.com.cn", "cms");
		ruleMap.put("cn.technode.com", "cms");
		ruleMap.put("it.sohu.com", "cms");
		ruleMap.put("digi.tech.qq.com", "cms");
		ruleMap.put("www.cio.com.cn", "cms");
		ruleMap.put("digi.163.com", "cms");
		ruleMap.put("digi.sina.com.cn", "cms");
		ruleMap.put("digi.it.sohu.com", "cms");
		ruleMap.put("www.36kr.com", "cms");
		ruleMap.put("news.ccidnet.com", "cms");
		ruleMap.put("www.donews.com", "cms");
		ruleMap.put("blog.csdn.net", "csdnblog");
		ruleMap.put("www.csdn.net", "csdnnews");

	}
	
	private String processUrl(String str) {

		try {
			String host = new URL(str).getHost();
			
			return ruleMap.get(host);

		} catch (MalformedURLException e) {
			e.printStackTrace();
		}

		return null;
	}

}