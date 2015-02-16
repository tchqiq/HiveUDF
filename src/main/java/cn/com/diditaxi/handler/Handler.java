package cn.com.diditaxi.handler;

import java.util.Map;


public abstract class Handler {
    
    protected Handler successor;

    public void setSuccesor(Handler successor) {
        this.successor = successor;
    }
    
    public abstract Map<String,String> HandleRequest(String request);

}
