package cn.com.diditaxi.hive.cf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import com.google.common.collect.Maps;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * 9/14/13 heqi
 */
@Description(name = "exref", 
	value = "_FUNC_(str) - 从ref为搜索引擎的url中抽取来源串",
	extended = "Example:\n"
		+ "  > SELECT exref(ref) FROM authors a;\n")
public final class ExtractRef extends UDF {
	static Map<String, String> ruleMap = Maps.newHashMap();

	public static void main(String[] args) {

	}

	private Text output = new Text();

	public Text evaluate(Text s) {
		output.set("");
		if (s == null) {
			return output;
		}
		String ref = s.toString();
		loadMap("refpty.properties");//装入规则至map
		output.set(exRef(ref));
		return output;
	}
	
	 /**
     * 处理来源
     * @param ref 来源url
     * @return 不在预定义范围内的来源则返回‘-’
     */
    private static String exRef(String ref){
        String source = "-";
        
        String[] strs = ref.split ("\\.");
        if(strs.length>1) {
            for(String s : strs) {
                if(ruleMap.containsKey (s)) {
                    source = ruleMap.get (s);
                }
            }
        }
        
        return source;
    }

    private static void loadMap(String rule){
        Properties prop = new Properties ();
        try {
            FileInputStream fis = new FileInputStream (rule);
            prop.load (fis);
            
            for(String name : prop.stringPropertyNames ()) {
                for(String s : prop.get (name).toString ().split (",")) {
                    ruleMap.put (s, name);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
