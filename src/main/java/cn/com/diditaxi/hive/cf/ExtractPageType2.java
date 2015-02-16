package cn.com.diditaxi.hive.cf;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;


@Description(name = "exrtactpagetype", 
			 value = "_FUNC_(str) - 根据curl判断页面类型相关数据",
			 extended = "")
public final class ExtractPageType2 extends UDF {

    public static void main(String[] args){
    	  
    	  ExtractPageType ep = new ExtractPageType(); 
          Text t = new Text(); 
          t.set("http://blog.csdn.net/zhangxin09/"); 
          System.out.println("start="+ep.evaluate(t)+"=end"); 
          t.set("http://blog.csdn.net/zhangyu_jsj?viewmode=contents"); 
          System.out.println("start="+ep.evaluate(t)+"=end"); 
          t.set("http://blog.csdn.net/zhaocj/"); 
          System.out.println("start="+ep.evaluate(t)+"=end"); 
          t.set("http://blog.csdn.net/zhaocj_09_tt/"); 
          System.out.println("start="+ep.evaluate(t)+"=end"); 
          t.set("http://blog.csdn.net/zhao4zhong1"); 
          System.out.println("start="+ep.evaluate(t)+"=end"); 
          t.set("http://blog.csdn.net/mobile/index.html"); 
          System.out.println("start="+ep.evaluate(t)+"=end"); 
          t.set("http://blog.csdn.net/column.html"); 
          System.out.println("start="+ep.evaluate(t)+"=end");
          t.set("http://blog.csdn.net/zzw_happy/article/details/1472407"); 
          System.out.println("start="+ep.evaluate(t)+"=end");
          t.set("http://blog.csdn.net/zzp_to_java/article/details/6959301"); 
          System.out.println("start="+ep.evaluate(t)+"=end");
          t.set("http://blog.csdn.net/zzz_781111/article/details/11133177"); 
          System.out.println("start="+ep.evaluate(t)+"=end"); 
          t.set("http://blog.csdn.net/kikitamoon/article/details/38223531#comments"); 
          System.out.println("start="+ep.evaluate(t)+"=end");
          t.set("http://blog.csdn.net/gamefish/article/list/%E6%8C%9A%E5%8F%8B%E7%9A%84blog%EF%BC%8Choho"); 
          System.out.println("start="+ep.evaluate(t)+"=end"); 
          
          
    }

    public String evaluate(Text s){
    	
        if(s == null) { return "";}
        String str = s.toString();
        return exportBlogPageType(str);
    }

    private String exportBlogPageType(String str){
    	
    	String pageType="";
    	int temp=str.indexOf("?");
    	if( temp != -1){
    		str=str.substring(0,temp);
    	}
    	str=str.replaceAll(":80","").replaceAll("//","/");
    	int lastInt=str.lastIndexOf("/");
    	if(lastInt == str.length()-1){
    		str=str.substring(0,lastInt);
    	}
    	try {
			str=URLDecoder.decode(str.toLowerCase(),"utf-8").trim();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
    	
    	if(str.indexOf("http:/write.blog.csdn.net/") != -1){
    		pageType = "writeBlog";
    	}else if(str.equals(BlogRegxStr.indexPage) ||str.equals(BlogRegxStr.indexPage2)){
    		pageType = "index";
    	}else if(str.equals(BlogRegxStr.rssPage)){
    		pageType = "rss";
    	}else if(str.equals(BlogRegxStr.helpPage)){
    		pageType = "help";
    	}else if(str.equals(BlogRegxStr.rulePage)){
    		pageType="rule";
    	}else if(str.equals(BlogRegxStr.rankingPage)){
    		pageType = "ranking";
    	}else if(str.equals(BlogRegxStr.registerPage)){
    		pageType="register";
    	}else if(str.equals(BlogRegxStr.addColumnPage)){            
    		pageType="addcolumn";
    	}else if(str.equals(BlogRegxStr.applyExpertPage)){
    		pageType="applyexpert";
    	}else if(str.equals(BlogRegxStr.applyPage)){
    		pageType="apply";
    	}else{
    		Pattern p = Pattern.compile (BlogRegxStr.errorPage);
            Matcher m = p.matcher (str);
            if(m.find()){
            	pageType = "error";
            }
            
            p = Pattern.compile (BlogRegxStr.hotNavigationPage);
            m = p.matcher(str);
            if(m.find()){
            	pageType = "hotNavigation";
            }
            p = Pattern.compile (BlogRegxStr.expertsNavigationPage);
            m = p.matcher(str);
            if(m.find()){
            	pageType = "expertsNavigation";
            }
            p = Pattern.compile (BlogRegxStr.columnNavigationPage);
            m = p.matcher (str);
            if(m.find()){
            	pageType = "columnNavigation";
            }
            p = Pattern.compile (BlogRegxStr.indexNavigationPage);
            m = p.matcher(str);
            if(m.find()){
            	pageType = "indexNavigation";
            }
            
            if(str.equals(BlogRegxStr.tagDetailsPage)){
            	pageType="tagDetails";
            }
            p = Pattern.compile (BlogRegxStr.columnDetailsPage);
            m = p.matcher(str);
            if(m.find()){
            	pageType = "columnDetails";
            }  
            p = Pattern.compile (BlogRegxStr.blogDetailsPage);
            m = p.matcher (str);
            if(m.find()){
            	pageType = "blogDetail";
            }
            p = Pattern.compile (BlogRegxStr.blogDetailsPage2);
            m = p.matcher (str);
            if(m.find()){
            	pageType = "blogDetail";
            }
            p = Pattern.compile (BlogRegxStr.blogDetailsPage3);
            m = p.matcher (str);
            if(m.find()){
            	pageType = "blogDetail";
            }
            p = Pattern.compile (BlogRegxStr.blogDetailsPage4);
            m = p.matcher (str);
            if(m.find()){
            	pageType = "blogDetail";
            }
            p = Pattern.compile (BlogRegxStr.blogDetailsPage5);
            m = p.matcher (str);
            if(m.find()){
            	pageType = "blogDetail";
            }
            p = Pattern.compile (BlogRegxStr.blogDetailsPage6);
            m = p.matcher (str);
            if(m.find()){
            	pageType = "blogDetail";
            }
            
    		p = Pattern.compile (BlogRegxStr.articlePage);
            m = p.matcher(str);
            if(m.find()){
            	pageType = "article";
            }
    	}
    	return pageType;
       
    }

    static class BlogRegxStr {
 
        public final static String indexPage="http:/blog.csdn.net"; //首页   
        public final static String indexPage2="http:/blog.csdn.net/default.html"; //首页  
        
        public final static String rankingPage    = "http:/blog.csdn.net/ranking.html";//	 排行
        public final static String helpPage    = "http:/blog.csdn.net/home/help.html";//	帮助
        public final static String rulePage    = "http:/blog.csdn.net/experts/rule.html";//专家申请规则页  
        public final static String rssPage    = "http:/blog.csdn.net/rss.html";//	订阅
        public final static String registerPage = "http:/blog.csdn.net/account/register.html";//注册页
        public final static String addColumnPage = "http:/blog.csdn.net/column/addcolumn.html";//申请专栏
        public final static String applyExpertPage = "http:/blog.csdn.net/experts/apply_blog_expert.html";//申请专家
        public final static String applyPage = "http:/blog.csdn.net/import/apply.html";//申请搬家

        public final static String errorPage = "http:/blog.csdn.net/error/(\\d+).html";//错误页
                        
        public final static String indexNavigationPage = "http:/blog.csdn.net(.*?)/(index|newest).html";//首页导航页
        public final static String columnNavigationPage = "http:/blog.csdn.net(.*?)/column([/list]*?).html";//专栏导航页                                                   
        public final static String expertsNavigationPage = "http:/blog.csdn.net(.*?)/experts.html";//专家导航页	
        public final static String hotNavigationPage = "http:/blog.csdn.net(.*?)/hot.html";//热点导航页
                                                        
  
        public final static String columnDetailsPage = "http:/blog.csdn.net/column/[details/]*?([A-Za-z0-9-]+?).html";//	专栏列表页
        public final static String tagDetailsPage = "http:/blog.csdn.net/tag/details.html";//	Tag列表页
        
        public final static String blogDetailsPage = "http:/blog.csdn.net/[_]*?[A-Za-z0-9]+?[_a-zA-Z0-9]*$";//	博主列表页                                             
        public final static String blogDetailsPage2 = "http:/blog.csdn.net/[a-z0-9]+?[_a-z0-9]*?/article";//	博主列表页
        public final static String blogDetailsPage3 = "http:/blog.csdn.net/[a-z0-9]+?[_a-z0-9]*?/article/list";//博主列表页
        public final static String blogDetailsPage4 = "http:/blog.csdn.net/([a-z0-9]+?[_a-z0-9]*?)/article/month/(\\d+)/(\\d+)";//	博主列表页
        public final static String blogDetailsPage5 = "http:/blog.csdn.net/([a-z0-9]+?[_a-z0-9]*?)/article/category/(\\d+)";//	博主列表页
        public final static String blogDetailsPage6 = "http:/blog.csdn.net/([a-z0-9]+?[_a-z0-9]*?)/article/list/(\\d+)";//	博主列表页
        
        public final static String articlePage = "http:/blog.csdn.net/(.*?)/article/details/(\\d+)";//文章页

    }
    
    static class BbsRegxStr {
    	 
        public final static String indexPage="http:/blog.csdn.net"; //首页   
        public final static String indexPage2="http:/blog.csdn.net/default.html"; //首页  
        
        public final static String rankingPage    = "http:/blog.csdn.net/ranking.html";//	 排行
        public final static String helpPage    = "http:/blog.csdn.net/home/help.html";//	帮助
        public final static String rulePage    = "http:/blog.csdn.net/experts/rule.html";//专家申请规则页  
        public final static String rssPage    = "http:/blog.csdn.net/rss.html";//	订阅
        public final static String registerPage = "http:/blog.csdn.net/account/register.html";//注册页
        public final static String addColumnPage = "http:/blog.csdn.net/column/addcolumn.html";//申请专栏
        public final static String applyExpertPage = "http:/blog.csdn.net/experts/apply_blog_expert.html";//申请专家
        public final static String applyPage = "http:/blog.csdn.net/import/apply.html";//申请搬家

        public final static String errorPage = "http:/blog.csdn.net/error/(\\d+).html";//错误页
                        
        public final static String indexNavigationPage = "http:/blog.csdn.net(.*?)/(index|newest).html";//首页导航页
        public final static String columnNavigationPage = "http:/blog.csdn.net(.*?)/column([/list]*?).html";//专栏导航页                                                   
        public final static String expertsNavigationPage = "http:/blog.csdn.net(.*?)/experts.html";//专家导航页	
        public final static String hotNavigationPage = "http:/blog.csdn.net(.*?)/hot.html";//热点导航页
                                                        
  
        public final static String columnDetailsPage = "http:/blog.csdn.net/column/[details/]*?([A-Za-z0-9-]+?).html";//	专栏列表页
        public final static String tagDetailsPage = "http:/blog.csdn.net/tag/details.html";//	Tag列表页
        
        public final static String blogDetailsPage = "http:/blog.csdn.net/[_]*?[A-Za-z0-9]+?[_a-zA-Z0-9]*$";//	博主列表页                                             
        public final static String blogDetailsPage2 = "http:/blog.csdn.net/[a-z0-9]+?[_a-z0-9]*?/article";//	博主列表页
        public final static String blogDetailsPage3 = "http:/blog.csdn.net/[a-z0-9]+?[_a-z0-9]*?/article/list";//博主列表页
        public final static String blogDetailsPage4 = "http:/blog.csdn.net/([a-z0-9]+?[_a-z0-9]*?)/article/month/(\\d+)/(\\d+)";//	博主列表页
        public final static String blogDetailsPage5 = "http:/blog.csdn.net/([a-z0-9]+?[_a-z0-9]*?)/article/category/(\\d+)";//	博主列表页
        public final static String blogDetailsPage6 = "http:/blog.csdn.net/([a-z0-9]+?[_a-z0-9]*?)/article/list/(\\d+)";//	博主列表页
        
        public final static String articlePage = "http:/blog.csdn.net/(.*?)/article/details/(\\d+)";//文章页

    }
    
    static class DownloadRegxStr {
    	 
        public final static String indexPage="http://download.csdn.net/"; //首页   
        public final static String indexPage2="http://download.csdn.net/?ref=toolbar_logo"; //首页  
        
        public final static String rankingPage    = "http://download.csdn.net/rankings";//	 排行
        public final static String helpPage    = "http://download.csdn.net/help";//	帮助
        
        public final static String docPage    = "http://download.csdn.net/doc";//精品文档
       

        public final static String errorPage = "http:/blog.csdn.net/error/(\\d+).html";//错误页
                        
        public final static String indexNavigationPage = "http:/blog.csdn.net(.*?)/(index|newest).html";//首页导航页
        public final static String columnNavigationPage = "http:/blog.csdn.net(.*?)/column([/list]*?).html";//专栏导航页                                                   
        public final static String expertsNavigationPage = "http:/blog.csdn.net(.*?)/experts.html";//专家导航页	
        public final static String hotNavigationPage = "http:/blog.csdn.net(.*?)/hot.html";//热点导航页
                                                        
  
        public final static String columnDetailsPage = "http:/blog.csdn.net/column/[details/]*?([A-Za-z0-9-]+?).html";//	专栏列表页
        public final static String tagDetailsPage = "http:/blog.csdn.net/tag/details.html";//	Tag列表页
        
        public final static String blogDetailsPage = "http:/blog.csdn.net/[_]*?[A-Za-z0-9]+?[_a-zA-Z0-9]*$";//	博主列表页                                             
        public final static String blogDetailsPage2 = "http:/blog.csdn.net/[a-z0-9]+?[_a-z0-9]*?/article";//	博主列表页
        public final static String blogDetailsPage3 = "http:/blog.csdn.net/[a-z0-9]+?[_a-z0-9]*?/article/list";//博主列表页
        public final static String blogDetailsPage4 = "http:/blog.csdn.net/([a-z0-9]+?[_a-z0-9]*?)/article/month/(\\d+)/(\\d+)";//	博主列表页
        public final static String blogDetailsPage5 = "http:/blog.csdn.net/([a-z0-9]+?[_a-z0-9]*?)/article/category/(\\d+)";//	博主列表页
        public final static String blogDetailsPage6 = "http:/blog.csdn.net/([a-z0-9]+?[_a-z0-9]*?)/article/list/(\\d+)";//	博主列表页
        
        public final static String articlePage = "http:/blog.csdn.net/(.*?)/article/details/(\\d+)";//文章页

    }

}
