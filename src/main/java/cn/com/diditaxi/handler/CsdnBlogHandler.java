package cn.com.diditaxi.handler;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import com.google.common.collect.Maps;


public class CsdnBlogHandler extends Handler {

    public final static String regexpStr = "^http://(blog.csdn.net)/([a-zA-Z0-9_]{2,20})/article/details/(\\d+)(\\?ref=.)*";
    
    @Override
    public Map<String, String> HandleRequest(String request){
        
        Map<String, String> output = Maps.newHashMap ();

        Pattern p = Pattern.compile (regexpStr);
        Matcher m = p.matcher (request);
        
        if (m.find ()) {
            
            output.put ("article", m.group (3));
            output.put ("type", "csdnblog");
            output.put ("author", m.group (2));
            
        } else if(successor != null){
            
            output = successor.HandleRequest (request);
            
        }
        
        return output;
    }

}
