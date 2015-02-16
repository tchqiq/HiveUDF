package cn.com.diditaxi.hive.cf;

import cn.com.diditaxi.handler.Handler;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by gyf on 14-11-27.
 */
public class AskTopicHander extends Handler{

    //http://ask.csdn.net/questions/156237
    public final static String regexpStr = "^http://ask\\.csdn\\.net/questions/(\\d+)$";


    @Override
    public Map<String, String> HandleRequest(String request) {
        Map<String, String> output = Maps.newHashMap();

        Pattern p = Pattern.compile(regexpStr);
        Matcher m = p.matcher(request);

        if (m.find()) {

            output.put("article", m.group(1));
            output.put("type", "ask_topic");
            output.put("author", "-");

        } else if (successor != null) {

            output = successor.HandleRequest(request);

        }

        return output;
    }
}
