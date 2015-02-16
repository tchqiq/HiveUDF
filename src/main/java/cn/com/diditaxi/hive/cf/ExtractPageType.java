package cn.com.diditaxi.hive.cf;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import com.google.common.base.Strings;


@Description(name = "exrtactpagetype", 
			 value = "_FUNC_(str) - 根据curl判断页面类型相关数据",
			 extended = "")
public final class ExtractPageType extends UDF {

    public static void main(String[] args){
    	
    	  ExtractPageType ep = new ExtractPageType(); 
          Text t = new Text(); 
         
          //t.set("http://download.csdn.net/tag/%E9%98%BB%E6%8A%97%E8%AE%BE%E8%AE%A1%E6%8C%87%E5%BC%95%E3%80%82%E4%BB%8B%E7%BB%8D%E5%8D%95%E7%AB%AF%E9%98%BB%E6%8A%97%E8%AE%BE%E8%AE%A1%EF%BC%8C%E5%B7%AE%E5%88%86%E9%98%BB%E6%8A%97%E8%AE%BE%E8%AE%A1%EF%BC%8C%E5%85%B1%E9%9D%A2%E9%98%BB%E6%8A%97%E8%AE%BE%E8%AE%A1%E4%B8%%8D%E7A7%8D%E7%BB%93%E6%9E%84%E7%9A%84%E9%98%BB%E6%8A%97%E8%AE%BE%E8%AE%A1%E3%80%82"); 
          //t.set("http://download.csdn.net/tag/%E6%9F%A5%E7%9C%8Bapk%E7%AD%BE%E5%90%8D%E4%BF%A");
          //t.set("http://download.csdn.net/#/tag/cygwin%E%AE%C%E%%B%E%AE%%E%A%%E%C%,cygwin%E%A%BB%E%BA%BF%E%AE%%E%A%%E%C%%E%B%B%E%BD%BD");
          //t.set("http://download.csdn.net/tag/Charles,..,%E%A%B%E%A%A,%E%C%%E%%B%E%%");
          //t.set("http://bbs.csdn.net/topics/390858594% aND 1236=1236 aND %=");
         // t.set("http://www.csdn.net/article/2015-01-08/2823476");
          t.set("http://job.csdn.net/job/index?jobid=324");
          System.out.println("start="+ep.evaluate(t)+"=end"); 
          
          
    }

    public String evaluate(Text s){
    	
        if(s == null) { return "";}
        String str = s.toString();
    	int temp=str.indexOf("?");
    	if(Strings.isNullOrEmpty(str)) { return "";}
    	if( temp != -1){
    		str=str.substring(0,temp);
    	}
    	str=str.replaceAll("#","").replaceAll(":8080","").replaceAll(":80","").replaceAll("//","/");
    	int lastInt=str.lastIndexOf("/");
    	if(lastInt != -1 && lastInt == str.length()-1){
    		str=str.substring(0,lastInt);
    	}
    	try {
			str=URLDecoder.decode(str.replaceAll("%", "%25").toLowerCase(),"utf-8").trim();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
    	String pageType="other";
    	Map<String,String []> regMap=new HashMap<String,String []>();
    	String hostStr=getHost(str);
    	if(hostStr.indexOf("blog.csdn.net") != -1){
    		BlogRegxStr.init();
    		regMap=BlogRegxStr.BlogRegMap;
    	}else if(hostStr.indexOf("bbs.csdn.net") !=-1){
    		BbsRegxStr.init();
    		regMap=BbsRegxStr.BbsRegMap;
    	}else if(hostStr.indexOf("download.csdn.net") !=-1){
    		DownloadRegxStr.init();
    		regMap=DownloadRegxStr.DownloadRegMap;
    		str=str.replaceAll("d.download.csdn.net","download.csdn.net")
    			   .replaceAll("z.download.csdn.net","download.csdn.net")
    			   .replaceAll("u.download.csdn.net","download.csdn.net");
    	}else if(hostStr.indexOf("ask.csdn.net") !=-1){
    		AskRegxStr.init();
    		regMap=AskRegxStr.AskRegMap;
    	}else if(hostStr.indexOf("huiyi.csdn.net") !=-1){
    		HuiyiRegxStr.init();
    		regMap=HuiyiRegxStr.HuiyiRegMap;
    	}else if(hostStr.indexOf("my.csdn.net") !=-1){
    		SpaceRegxStr.init();
    		regMap=SpaceRegxStr.SpaceRegMap;
    	}else if(hostStr.indexOf("news.csdn.net") !=-1 || hostStr.indexOf("www.csdn.net") !=-1){
    		NewsRegxStr.init();
    		regMap=NewsRegxStr.NewsRegMap;
    	}else if(hostStr.indexOf("biz.csdn.net") !=-1 || hostStr.indexOf("job.csdn.net") !=-1){
    		JobRegxStr.init();
    		regMap=JobRegxStr.JobRegMap;
    	}
    	
        for(Map.Entry<String,String[]> entry:regMap.entrySet()){
        	String tempStr=entry.getKey();
        	tempStr=tempStr.split("#")[1];
        	if("equals".equals(entry.getValue()[0])){
        		if(str.equals(tempStr)){
        			pageType = entry.getValue()[1];
        			break;
        		}
        	}else if("indexof".equals(entry.getValue()[0])){
        		if(str.indexOf(tempStr) != -1){
        			pageType = entry.getValue()[1];
        			break;
        		}
        	}else if("matcher".equals(entry.getValue()[0])){
            	Pattern p = Pattern.compile(tempStr);
                Matcher m = p.matcher (str);
                if(m.find()){
                	pageType = entry.getValue()[1];
                	break;
                }
        	}
        }
    	return pageType; 
    }
    
    static class BlogRegxStr {
        
    	public static Map<String,String []> BlogRegMap=new TreeMap<String,String[]>();
    	public static void init(){
    		BlogRegMap.put("01#http:/blog.csdn.net",new String[]{"equals","index"});                                   //首页 
    		BlogRegMap.put("02#http:/blog.csdn.net/default.html",new String[]{"equals","index"});                      //首页 
    		
    		BlogRegMap.put("03#http:/blog.csdn.net/ranking.html",new String[]{"equals","ranking"});                    //排行
    		BlogRegMap.put("04#http:/blog.csdn.net/home/help.html",new String[]{"equals","help"});                     //帮助
    		BlogRegMap.put("05#http:/write.blog.csdn.net",new String[]{"indexof","writeBlog"});                        //博客发表页
    		BlogRegMap.put("06#http:/blog.csdn.net/experts/rule.html",new String[]{"equals","rule"});                  //专家申请规则页 
    		BlogRegMap.put("07#http:/blog.csdn.net/rss.html",new String[]{"equals","rss"});                            //订阅
    		BlogRegMap.put("08#http:/blog.csdn.net/account/register.html",new String[]{"equals","register"});          //注册页
    		BlogRegMap.put("09#http:/blog.csdn.net/column/addcolumn.html",new String[]{"equals","addColumn"});           //申请专栏
    		BlogRegMap.put("10#http:/blog.csdn.net/experts/apply_blog_expert.html",new String[]{"equals","applyExpert"});//申请专家
    		BlogRegMap.put("11#http:/blog.csdn.net/import/apply.html",new String[]{"equals","applyHome"});               //申请搬家
    		BlogRegMap.put("12#http:/blog.csdn.net/error/(\\d+).html",new String[]{"matcher","error"});                   //错误页
    		
    		BlogRegMap.put("13#http:/blog.csdn.net(.*?)/(index|newest).html",new String[]{"matcher","indexNavigation"});  //首页导航页	                   
    		BlogRegMap.put("14#http:/blog.csdn.net(.*?)/(column.html)$",new String[]{"matcher","columnNavigation"});      //专栏导航页
    		BlogRegMap.put("15#http:/blog.csdn.net(.*?)/(column/list.html)$",new String[]{"matcher","columnNavigation"}); //专栏导航页
    		BlogRegMap.put("16#http:/blog.csdn.net(.*?)/experts.html",new String[]{"matcher","expertsNavigation"});         //专家导航页	
    		BlogRegMap.put("17#http:/blog.csdn.net(.*?)/hot.html",new String[]{"matcher","hotNavigation"});                 //热点导航页
    		
    		BlogRegMap.put("18#http:/blog.csdn.net/column/manage.html",new String[]{"equals","columnList"});//专栏列表页
    		BlogRegMap.put("19#http:/blog.csdn.net/column/details/([A-Za-z0-9-]+?).html",new String[]{"matcher","columnList"});//专栏列表页
    		BlogRegMap.put("20#http:/blog.csdn.net/tag/details.html",new String[]{"matcher","tagList"});                          //Tag列表页
    		BlogRegMap.put("21#http:/blog.csdn.net/[_]*?[A-Za-z0-9]+?[_a-zA-Z0-9]*$",new String[]{"matcher","blogList"});         //博主列表页
    		BlogRegMap.put("22#http:/blog.csdn.net/[a-z0-9]+?[_a-z0-9]*?/(article)$",new String[]{"matcher","blogList"});            //博主列表页
    		BlogRegMap.put("23#http:/blog.csdn.net/[a-z0-9]+?[_a-z0-9]*?/article/list",new String[]{"matcher","blogList"});       //博主列表页
    		BlogRegMap.put("24#http:/blog.csdn.net/([a-z0-9]+?[_a-z0-9]*?)/article/month/(\\d+)/(\\d+)",new String[]{"matcher","blogList"});//博主列表页
    		BlogRegMap.put("25#http:/blog.csdn.net/([a-z0-9]+?[_a-z0-9]*?)/article/category/(\\d+)",new String[]{"matcher","blogList"});//博主列表页
    		BlogRegMap.put("26#http:/blog.csdn.net/([a-z0-9]+?[_a-z0-9]*?)/article/list/(\\d+)",new String[]{"matcher","blogList"});//博主列表页
    		
    		BlogRegMap.put("27#http:/blog.csdn.net/(.*?)/article/details/(\\d+)",new String[]{"matcher","article"});//文章页
    	}
    }
    
    static class BbsRegxStr {
    	 
    	public static Map<String,String []> BbsRegMap=new TreeMap<String,String []>();
    	
    	public static void init(){
    		BbsRegMap.put("01#http:/bbs.csdn.net/home",new String[]{"equals","index"});//首页
    		BbsRegMap.put("02#http:/bbs.csdn.net",new String[]{"equals","index"});//首页
    		BbsRegMap.put("03#http:/bbs.csdn.net/help",new String[]{"equals","help"});//	帮助
    		BbsRegMap.put("04#http:/bbs.csdn.net/rank",new String[]{"equals","rank"});//排行页(牛人页)
    		BbsRegMap.put("05#http:/bbs.csdn.net/ask",new String[]{"equals","ask"});//专家问答
    		
    		BbsRegMap.put("06#http:/bbs.csdn.net/error/(\\d+).html",new String[] {"matcher","error"});// 错误页
    		BbsRegMap.put("07#http:/bbs.csdn.net/user/point",new String[]{"equals","point"});//用户积分页
    		BbsRegMap.put("08#http:/bbs.csdn.net/user/resource_point_transfer",new String[]{"equals","pointTransfer"});//用户积分兑换页
    		BbsRegMap.put("09#http:/bbs.csdn.net/user/open_topics",new String[]{"equals","openTopics"});//用户未结帖子页
    		BbsRegMap.put("10#http:/bbs.csdn.net/user/replied_topics",new String[]{"equals","openTopics"});//用户回复帖子页
    		BbsRegMap.put("11#http:/bbs.csdn.net/topics/new",new String[]{"indexof", "addTopic"});//发表帖子页
    		
    		BbsRegMap.put("12#http:/bbs.csdn.net/map",new String[]{"matcher", "mapNavigation"});//地图导航页
    		
    		BbsRegMap.put("13#http:/bbs.csdn.net/forums/([A-Za-z0-9-]+?)",new String[]{"matcher", "forumsList"});//	版块列表页
    		BbsRegMap.put("14#http:/bbs.csdn.net/users/(.*?)/(topics)$",new String[]{"matcher","userList"});//用户帖子列表页
    		
    		BbsRegMap.put("15#http:/bbs.csdn.net/topics/(\\d+)",new String[]{"matcher", "topic"});//帖子页
    	}
    }
    
    static class DownloadRegxStr {
    	
    	public static Map<String,String []> DownloadRegMap=new TreeMap<String,String[]>();
    	
    	public static void init(){
    		DownloadRegMap.put("01#http:/download.csdn.net",new String[]{"equals","index"}); //首页 
    		DownloadRegMap.put("02#http:/download.csdn.net/",new String[]{"equals","index"}); //首页 
    		
    		DownloadRegMap.put("03#http:/download.csdn.net/rankings",new String[]{"indexof","ranking"});//	 排行
    		DownloadRegMap.put("04#http:/download.csdn.net/help",new String[]{"equals","help"});//	帮助
    		DownloadRegMap.put("05#http:/download.csdn.net/error/(\\d+).html",new String[]{"matcher","error"});//错误页
    		DownloadRegMap.put("06#http:/download.csdn.net/upload",new String[]{"indexof","upload"});//上传页
    		DownloadRegMap.put("07#http:/download.csdn.net/advanced_search",new String[]{"indexof","advancedSearch"});//高级搜索页	
    		
    		DownloadRegMap.put("08#http:/download.csdn.net/category",new String[]{"matcher","categoryNavigation"});//资源分类导航页   
    		DownloadRegMap.put("09#http:/download.csdn.net/album/list/(\\d+)",new String[]{"matcher","albumNavigation"});//专辑导航页    
    		DownloadRegMap.put("10#http:/download.csdn.net/doc",new String[]{"matcher","docNavigation"});//精品文档导航页	
    		
    		DownloadRegMap.put("11#http:/download.csdn.net/c-(\\d+)",new String[]{"matcher","categoryList"});//	资源分类列表页
    		DownloadRegMap.put("12#http:/download.csdn.net/album/detail/(\\d+)",new String[]{"matcher","docList"});//专辑列表页
    		DownloadRegMap.put("13#http:/download.csdn.net/my",new String[]{"indexof","myList"});//	我的列表页
    		DownloadRegMap.put("14#http:/download.csdn.net/tag",new String[]{"indexof","tagList"});//	tag列表页
    		DownloadRegMap.put("15#http:/download.csdn.net/user/",new String[]{"indexof","userList"});//	用户列表页
    		DownloadRegMap.put("16#http:/download.csdn.net/search",new String[]{"indexof","searchList"});//	搜索列表页
    		
    		DownloadRegMap.put("17#http:/download.csdn.net/detail/(.*?)/(\\d+)",new String[]{"matcher","detail"});//详情页
    		DownloadRegMap.put("18#http:/download.csdn.net/download/(.*?)/(\\d+)",new String[]{"matcher","download"});//下载页
    		DownloadRegMap.put("19#http:/download.csdn.net/downloadrec/(.*?)/(\\d+)",new String[]{"matcher","downloadrec"});//下载完成页
    	}
    }
    
    static class AskRegxStr {
   	 
    	public static Map<String,String []> AskRegMap=new TreeMap<String,String []>();
    	
    	public static void init(){
    		AskRegMap.put("01#http:/ask.csdn.net",new String[]{"equals","index"});//问答首页
    		AskRegMap.put("02#http:/ask.csdn.net/my",new String[]{"indexof", "my"});//我的问答
    		AskRegMap.put("03#http:/ask.csdn.net/scores",new String[]{"indexof", "scores"});//荣誉殿堂 
    		AskRegMap.put("04#http:/ask.csdn.net/experts",new String[]{"indexof", "experts"});//热心人 
    		AskRegMap.put("05#http:/ask.csdn.net/mentors",new String[]{"indexof","mentors"});//问答导师
    		AskRegMap.put("06#http:/ask.csdn.net/questions/(\\d+)",new String[]{"matcher", "detail"});//问题详情页 
    		AskRegMap.put("07#http:/ask.csdn.net/questions/tags",new String[]{"indexof", "tags"});//问题tag页
    	}
    }
    
    static class SpaceRegxStr {
      	 
    	public static Map<String,String []> SpaceRegMap=new TreeMap<String,String []>();
    	
    	public static void init(){
    		SpaceRegMap.put("01#http:/my.csdn.net",new String[]{"equals","myindex"});//首页
    		SpaceRegMap.put("02#http:/my.csdn.net/my/mycsdn",new String[]{"equals","mycsdn"});//首页
    		SpaceRegMap.put("03#http:/my.csdn.net/(.+\\w$)",new String[]{"matcher","index"});//主页链接
    		//SpaceRegMap.put("03#http:/msg.csdn.net/letter",new String[]{"equals","message"});//首页
    	}
    }
    
    static class HuiyiRegxStr{
    	public static Map<String,String []> HuiyiRegMap=new TreeMap<String,String []>();
    	
    	public static void init(){
    		HuiyiRegMap.put("01#http:/huiyi.csdn.net",new String[]{"equals","index"});//首页
    		HuiyiRegMap.put("02#http:/huiyi.csdn.net/activity/home",new String[]{"equals","index"});//首页
    		HuiyiRegMap.put("03#http:/huiyi.csdn.net/m/activity/activity/index",new String[]{"equals","index"});//首页
    		
    		HuiyiRegMap.put("04#http:/huiyi.csdn.net/activity/home/biz",new String[]{"equals","bizList"});//CSDN主办
    		HuiyiRegMap.put("05#http:/huiyi.csdn.net/m/activity/activity/index/biz",new String[]{"equals","bizList"});//CSDN主办
    		HuiyiRegMap.put("06#http:/huiyi.csdn.net/activity/home/tech",new String[]{"equals","techList"});//业界活动
    		HuiyiRegMap.put("07#http:/huiyi.csdn.net/m/activity/activity/index/tech",new String[]{"equals","techList"});//业界活动
    		HuiyiRegMap.put("08#http:/huiyi.csdn.net/activity/home/cto",new String[]{"equals","ctoList"});//CTO俱乐部
    		HuiyiRegMap.put("09#http:/huiyi.csdn.net/activity/home/tup",new String[]{"equals","tupList"});//TUP活动
    		HuiyiRegMap.put("10#http:/huiyi.csdn.net/activity/home/community",new String[]{"equals","communityList"});//社区
    		HuiyiRegMap.put("11#http:/huiyi.csdn.net/activity/home/online",new String[]{"equals","inlineList"});//在线活动
    		HuiyiRegMap.put("12#http:/huiyi.csdn.net/m/activity/activity/index/online",new String[]{"equals","onlineList"});//在线活动
				    		
    		HuiyiRegMap.put("13#http:/huiyi.csdn.net/activity/product/free",new String[]{"equals","signup"});//申请参会
    		HuiyiRegMap.put("14#http:/huiyi.csdn.net/module/activity/product/free",new String[]{"equals","signup"});//申请参会
    		HuiyiRegMap.put("15#http:/huiyi.csdn.net/activity/product/my_activity",new String[]{"equals","myactivity"});//我的活动
    		
    		HuiyiRegMap.put("16#http:/huiyi.csdn.net/activity/product/goods_list",new String[]{"equals","activity"});//活动页
    		HuiyiRegMap.put("17#http:/huiyi.csdn.net/m/activity/product/goods_list",new String[]{"equals","activity"});//活动页
    		HuiyiRegMap.put("18#http:/huiyi.csdn.net/activity/closed",new String[]{"equals","activity"});//活动页	
         }
    }
    
    static class NewsRegxStr{
    	public static Map<String,String []> NewsRegMap=new TreeMap<String,String []>();
    	
    	public static void init(){
    		NewsRegMap.put("01#http:/news.csdn.net",new String[]{"equals","index"});//首页
    		NewsRegMap.put("02#^http:/(www.csdn.net)/article/[^tag/](\\d+)",new String[]{"matcher","news"});//资讯页
         }
    }
    
    static class JobRegxStr{
    	public static Map<String,String []> JobRegMap=new TreeMap<String,String []>();
    	
    	public static void init(){
    		JobRegMap.put("01#http:/biz.csdn.net.*",new String[]{"matcher","biz"});//企业
    		JobRegMap.put("02#http:/(job.csdn.net)/job/index.*?",new String[]{"matcher","jobdetail"});//职位详情
    		JobRegMap.put("03#http:/job.csdn.net.*",new String[]{"matcher","job"});//job
         }
    } 
    
    public static String getHost(String url){
    	  if(url==null||url.trim().equals("")){
    	   return "";
    	  }
    	  String host = "";
    	  Pattern p =  Pattern.compile("(?<=//|)((\\w)+\\.)+\\w+");
    	  Matcher matcher = p.matcher(url); 
    	  if(matcher.find()){
    	   host = matcher.group(); 
    	  }
    	  return host;
    }
}
