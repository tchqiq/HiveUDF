package cn.com.diditaxi.hive.cf;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import com.google.common.base.Strings;

@Description(name = "expro", 
			 value = "_FUNC_(str,rule) - 根据规则抽取产品类别",
			 extended = "")
public final class ExtractProduct2 extends UDF {
	
	private static TreeMap<String, String> ruleMap = new TreeMap<String, String>(
			new Comparator<String>(){
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
	
	public Text evaluate(Text s,Text rule){
		output.set("LogDataException");
		if(s == null || Strings.isNullOrEmpty(s.toString()) || "-".equals(s.toString())){
			return output;
		}
		String str = s.toString();
		//加载规则
        Properties prop = new Properties ();
        try {
            String path = "/" + rule;
            prop.load(ExtractProduct2.class.getResourceAsStream(path));
            for(String name : prop.stringPropertyNames ()) {
                ruleMap.put (name, prop.getProperty(name));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        str=str.split("\\?")[0];
		output.set(exPro(str));
		return output;
	}
	
	/**
	 * 抽取预定义好的产品分类
	 * @param str
	 * @return
	 */
    private static String exPro(String str){
    	String product = ruleMap.get("N");
    	//如果是url
    	if("url".equals(ruleMap.get("type"))){
    		if(str.startsWith("http://") || str.startsWith("https://")){
    			try{
    				URL url = new URL(str);
    				 String hostName = url.getHost();
    				 String key="";
    				 String value="";
    				 Pattern p=null;
    				 Matcher matcher=null;
    				 for(Map.Entry<String,String> entry:ruleMap.entrySet()){
    					 key=entry.getKey();
    					 value=entry.getValue();
    					 if(key.indexOf(";") != -1){
    						 if(key.split(";")[0].equals("equals")){
    							 if(str.equals(key.split(";")[1])){
    								product = value;
       				    		  	break;
    							 }
    						 }
    					 }else if(key.contains("*")){
    						  key=key.replaceAll("\\*","(.*?)");
    						  p =  Pattern.compile(key);
    				    	  matcher = p.matcher(str); 
    				    	 if(matcher.find()){
    				    		  product = value;
    				    		  break;
    				    	  }
    					 }else if(key.contains("/")){
    						 str=str.replaceAll("http://","").replaceAll("https://","");
    						 if(str.startsWith(key)){
    							 product=value;
    							 break;
    						 }
    					 }else{
    						 if(key.equals(hostName)){
    				    		product = value;
    				    		break;
    				    	 }
    					 }
    				 }				
    			}catch (MalformedURLException e) {
    				e.printStackTrace();
    			}
        	}
    	}else{
    		if(ruleMap.containsKey(str)){
    			product = ruleMap.get(str);
    		}
    	}
        return product;
    }
    
	public static void main(String[] args) {
		ExtractProduct2 ep = new ExtractProduct2();
		Text t = new Text();
		Text tt = new Text();
		t.set("http://www.csdn.net//headlines.html");
		tt.set("pid3.properties");
		System.out.println(ep.evaluate(t,tt).toString());
	}
}
