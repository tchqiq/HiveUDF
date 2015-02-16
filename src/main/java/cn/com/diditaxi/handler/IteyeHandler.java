package cn.com.diditaxi.handler;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Maps;

public class IteyeHandler extends Handler {
    
    public final static String regexpStr = "^http://(.+).iteye.com/(news|blog)/(\\d+)";

    @Override
    public Map<String, String> HandleRequest(String request){

        Map<String, String> output = Maps.newHashMap ();

        Pattern p = Pattern.compile (regexpStr);
        Matcher m = p.matcher (request);

        if (m.find ()) {

            String type = m.group (2);
            
            if ("news".equals (type)) {
                output.put ("article", m.group (3));
                output.put ("type", "iteyenews");
                output.put ("author", "iteyenews");
            } else if("blog".equals (type)){
                output.put ("article", m.group (3));
                output.put ("type", "iteyeblog");
                output.put ("author", m.group (1));
            }

        } else if (successor != null) {

            output =  successor.HandleRequest (request);

        }

        return output;
    }

}
