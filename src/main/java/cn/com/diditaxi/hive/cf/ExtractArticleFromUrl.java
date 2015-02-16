package cn.com.diditaxi.hive.cf;

import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import cn.com.diditaxi.handler.BBSHandler;
import cn.com.diditaxi.handler.CsdnBlogHandler;
import cn.com.diditaxi.handler.CsdnNewsHandler;
import cn.com.diditaxi.handler.DownloadHandler;
import cn.com.diditaxi.handler.Handler;
import cn.com.diditaxi.handler.IteyeHandler;

import com.google.common.collect.Maps;

/**
 * 9/14/13 heqi
 */
@Description(name = "exarticle", 
			 value = "_FUNC_(str) - 根据url抽取article相关数据",
			 extended = "Example:\n"
	+ "  > SELECT exarticle(url)['article'] FROM file_pv_track;\n")
public final class ExtractArticleFromUrl extends UDF {

    public static void main(String[] args){

        ExtractArticleFromUrl ep = new ExtractArticleFromUrl ();
        Text t = new Text ();
        t.set ("http://ask.csdn.net/questions/156237");
        System.out.println (ep.evaluate (t));

    }

    private Map<String, String> output = Maps.newHashMap ();

    public Map<String, String> evaluate(Text s){
        output = null;
        if (s == null) { return output; }

        String str = s.toString ();

        Handler csdnblogHandler = new CsdnBlogHandler ();
        Handler csdnnewsHandler = new CsdnNewsHandler ();
        Handler iteyeHandler = new IteyeHandler ();
        Handler downloadHandler = new DownloadHandler ();
        Handler bbsHandler = new BBSHandler ();
        Handler codeHandler = new CodeHander ();
        Handler askTopicHandler = new AskTopicHander();

        csdnblogHandler.setSuccesor (csdnnewsHandler);
        csdnnewsHandler.setSuccesor (iteyeHandler);
        iteyeHandler.setSuccesor (downloadHandler);
        downloadHandler.setSuccesor(bbsHandler);
        bbsHandler.setSuccesor(codeHandler);
        codeHandler.setSuccesor(askTopicHandler);

        output = csdnblogHandler.HandleRequest (str);

        return output;
    }

}
