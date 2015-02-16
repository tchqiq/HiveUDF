package cn.com.diditaxi.handler;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Maps;

public class DownloadHandler extends Handler {
    
    public final static String regexpStr = "^http://download.csdn.net/(detail|download)/([a-zA-Z0-9_]{2,20})/(\\d+$)";

    @Override
    public Map<String, String> HandleRequest(String request){

        Map<String, String> output = Maps.newHashMap ();

        Pattern p = Pattern.compile (regexpStr);
        Matcher m = p.matcher (request);

        if (m.find ()) {

            if ("detail".equals (m.group (1))) {
                output.put ("article", m.group (3));
                output.put ("type", "detail");
                output.put ("author", m.group (2));
            } else if("download".equals (m.group (1))){
                output.put ("article", m.group (3));
                output.put ("type", "download");
                output.put ("author", m.group (2));
            }

        } else if (successor != null) {

            output = successor.HandleRequest (request);

        }

        return output;
    }

}
