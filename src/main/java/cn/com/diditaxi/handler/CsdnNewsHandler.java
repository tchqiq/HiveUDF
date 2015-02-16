package cn.com.diditaxi.handler;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import com.google.common.collect.Maps;


public class CsdnNewsHandler extends Handler {

    public final static String regexpStr = "^http://(www.csdn.net)/article/(.+)/(\\d+)(\\?ref=.)*";
    
    @Override
    public Map<String, String> HandleRequest(String request){
        
        Map<String, String> output = Maps.newHashMap ();

        Pattern p = Pattern.compile (regexpStr);
        Matcher m = p.matcher (request);
        
        if (m.find ()) {
            
            output.put ("article", m.group (3));
            output.put ("type", "csdnnews");
            output.put ("author", "csdn" + m.group (2));
            
        } else if(successor != null){
            
            output = successor.HandleRequest (request);
            
        }
        
        return output;
    }

}
