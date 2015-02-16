package cn.com.diditaxi.hive.cf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.Properties;

/**
 * 匹配xml配置
 */
@Description(name = "expro", 
			 value = "_FUNC_(str,rule) - 根据规则抽取产品类别",
			 extended = "Example:\n"
	+ "  > SELECT expro(url,rule) FROM authors a;\n")
public final class ExtractFromXML extends UDF {
	static Map<String, String> ruleMap = Maps.newHashMap();
	
	public static void main(String[] args) {
		ExtractFromXML ep = new ExtractFromXML();
		Text t = new Text();
		Text tt = new Text();
		t.set("foo");
		tt.set("test.xml");
		System.out.println(ep.evaluate(t,tt).toString());
	}

	private Text output = new Text();

	public Text evaluate(Text s,Text rule) {
    	ruleMap.clear();
		output.set("LogDataException");
		if (s == null) {
			return output;
		}
		String str = s.toString();
		loadMap(rule.toString());//装入规则至map
		output.set(exPro(str)==null?"LogDataException":exPro(str));
		return output;
	}
	
	
	
	/**
	 * 抽取预定义好的产品分类
	 * @param str
	 * @return
	 */
    private static String exPro(String str){
    	String product = ruleMap.get("N");
    	if(Strings.isNullOrEmpty(str)) return "LogDataException";
    	
    	//如果是url
    	if("url".equals(ruleMap.get("type"))) {
    		if(str.startsWith("http://")||str.startsWith("https://")) {
        		product = processUrl(str);
        	}else {
        		if("-".equals(str)) {
        			product = ruleMap.get("-");
        		}else {
        			product = "LogDataException";
        		}
        	}
    	}
    	
    	
    	else {
    		if(ruleMap.containsKey (str)) {
    			product = ruleMap.get (str);
    		}
    	}
       
        return product;
    }

    private static String processUrl(String str) {
    	String product = ruleMap.get("N");
    	URL url;
		try {
			 url = new URL(str);
			 String hostName = url.getHost();
			 if(str.contains(".iteye.")) {
				 return ruleMap.get("www.iteye.com");
			 }
			
			 if(ruleMap.containsKey (hostName)) {
				 if("www.csdn.net".equals(hostName)) {
					 return processCsdn(str);
				 }
				 if("club.csdn.net".equals(hostName)) {
					 return processClub(str);
				 }
				 return ruleMap.get (hostName);
			 }				
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		return product;
	}

	private static String processClub(String str) {
		if(str.startsWith("http://club.csdn.net/cto/")) {
			 return ruleMap.get("club.csdn.net/cto");
		 }
		 if(str.startsWith("http://club.csdn.net/student/")) {
			 return ruleMap.get("club.csdn.net/student");
		 }
		 if(str.startsWith("http://club.csdn.net/cmdn/")) {
			 return ruleMap.get("club.csdn.net/cmdn");
		 }
		 if(str.startsWith("http://club.csdn.net/community/")) {
			 return ruleMap.get("club.csdn.net/community");
		 }
		 if(str.startsWith("http://club.csdn.net/hr/")) {
			 return ruleMap.get("club.csdn.net/hr");
		 }
		 if("http://www.csdn.net/".equals(str)) {
			 return ruleMap.get("www.csdn.net");
		 }
		 return "clubException";
	}

	private static String processCsdn(String str) {
		 if(str.startsWith("http://www.csdn.net/article/lastnews/")) {
			 return ruleMap.get("www.csdn.net/article/lastnews");
		 }
		 if(str.startsWith("http://www.csdn.net/article/tag/")) {
			 return ruleMap.get("www.csdn.net/article/tag");
		 }
		 if(str.startsWith("http://www.csdn.net/article/")) {
			 return ruleMap.get("www.csdn.net/article");
		 }
		 if(str.startsWith("http://www.csdn.net/tag/")) {
			 return ruleMap.get("www.csdn.net/tag");
		 }
		 if("http://www.csdn.net/".equals(str)) {
			 return ruleMap.get("www.csdn.net");
		 }
		 return "csdnException";
	}

	private static void loadMap(String rule){
        Properties prop = new Properties ();
        try {
            String path = "/" + rule;
            prop.loadFromXML(ExtractFromXML.class.getResourceAsStream(path));

            for(String name : prop.stringPropertyNames ()) {
                ruleMap.put (name, prop.getProperty(name));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
	
	


}
