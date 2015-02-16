package cn.com.diditaxi.handler;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import com.google.common.collect.Maps;


public class BBSHandler extends Handler {

    public final static String regexpStr = "^http://bbs.csdn.net/topics/(\\d+$)";
    
    @Override
    public Map<String, String> HandleRequest(String request){
        
        Map<String, String> output = Maps.newHashMap ();

        Pattern p = Pattern.compile (regexpStr);
        Matcher m = p.matcher (request);
        
        if (m.find ()) {
            
            output.put ("article", m.group (1));
            output.put ("type", "bbs");
            output.put ("author", "-");
            
        } else if(successor != null){
            
            output = successor.HandleRequest (request);
            
        }
        
        return output;
    }

}
