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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 9/14/13 heqi
 */
@Description(name = "exdownload", 
			 value = "_FUNC_(str) - 根据curl抽取download相关数据",
			 extended = "Example:\n"
	+ "  > SELECT exdownload(url) FROM file_pv_track a;\n")
public final class ExtractDownload extends UDF {
	
	public static void main(String[] args) {
		/*ExtractDownload ep = new ExtractDownload();
		Text t = new Text();
		t.set("http://download.csdn.net/detail/hxc2008q/6703111");
		
		System.out.println(ep.evaluate(t));*/
	}

	private Map<String,String> output = Maps.newHashMap();

	public Map<String,String> evaluate(Text s) {
		output = null;
		if (s == null) {
			return output;
		}
		String str = s.toString();
		output = exportDownload(str);
		return output;
	}
	
	
	
	private Map<String, String> exportDownload(String str) {
		Map<String,String> output = Maps.newHashMap();
		
		Pattern p = Pattern.compile("http://download.csdn.net/detail/(.+)/(.+)");
		Matcher m = p.matcher(str);
		if(m.find()) {
			output.put("article", m.group(2));
			output.put("type", "download");
			output.put("author", m.group(1));
		} else {
			return null;
		}
		return output;
	}

}
