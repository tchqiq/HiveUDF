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
@Description(name = "exarticle", 
			 value = "_FUNC_(str) - 根据curl抽取article相关数据",
			 extended = "Example:\n"
	+ "  > SELECT expro(url,rule) FROM authors a;\n")
public final class ExtractArticle extends UDF {

    public static void main(String[] args){
        /*
         * ExtractArticle ep = new ExtractArticle(); Text t = new Text(); t.set("http://www.iteye.com/news/28371"); System.out.println(ep.evaluate(t));
         */
    }

    private Map<String, String> output = Maps.newHashMap ();

    public Map<String, String> evaluate(Text s){
        output = null;
        if (s == null) { return output; }
        String str = s.toString ();
        output = exportArticle (str);
        return output;
    }

    private Map<String, String> exportArticle(String str){
        Map<String, String> output = Maps.newHashMap ();

        Pattern p = Pattern.compile (RegxStr.csdnblog);
        Matcher m = p.matcher (str);
        if (m.find ()) {
            output.put ("article", m.group (3));
            output.put ("type", "csdnblog");
            output.put ("author", m.group (2));
        } else {
            p = Pattern.compile (RegxStr.csdnnews);
            m = p.matcher (str);
            if (m.find ()) {
                output.put ("article", m.group (3));
                output.put ("type", "csdnnews");
                output.put ("author", "csdn" + m.group (2));
            } else {
                p = Pattern.compile (RegxStr.iteye);
                m = p.matcher (str);
                if (m.find ()) {
                    if ("www".equals (m.group (1))) {
                        output.put ("article", m.group (3));
                        output.put ("type", "iteyenews");
                        output.put ("author", "iteye" + m.group (2));
                    } else {
                        output.put ("article", m.group (3));
                        output.put ("type", "iteyeblog");
                        output.put ("author", m.group (1));
                    }
                } else {
                    return null;
                }
            }
        }
        return output;
    }

    static class RegxStr {

        public final static String csdnblog = "http://(blog.csdn.net)/(.+)/article/details/(\\d+)";
        public final static String csdnnews = "http://(www.csdn.net)/article/(.+)/(\\d+)";
        public final static String iteye    = "http://(.+).iteye.com/(news|blog)/(\\d+)";
    }

}
