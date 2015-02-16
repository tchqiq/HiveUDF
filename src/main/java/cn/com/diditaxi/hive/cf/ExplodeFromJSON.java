package cn.com.diditaxi.hive.cf;

import net.sf.json.JSONObject;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 1/26/14 WilliamZhu(allwefantasy@gmail.com)
 */
@Description(
        name = "u_json",
        value = "_FUNC_(str,key) - 从json格式数据中获取Key,默认为-",
        extended = "Example:\n" +
                "  > SELECT ParseJson('data','pid') FROM file_pv_track a;\n"
)
public class ExplodeFromJSON extends UDF {
    private Text output = new Text("-");
    
//    public static void main(String[] args) {
//		ExplodeFromJSON efj = new ExplodeFromJSON();
//		System.out.println(efj.evaluate(new Text("{\"body\":\"\r\n1、先进先出法 \r\n先进先出法是假定先收到的存货先发出或先收到的存货先耗用，并根据这种假定的存货流转次序对发出存货和期束存货进行计价的一种方法。 \r\n根据谨慎性原则的要求，先进先出法适用于市场价格普遍处于下降趋势的商品。因为采用先进先出法，期末存货的帐面价格反映的是最后购进的较低的价格，对于市场价格处于下降趋势的产品，符合谨慎原则的要求，能抵御物价下降的影响，减少企业经营的风险，消除了潜亏隐患，从而避免了由于存货资金不实而虚增企业帐面资产。这时如果采用后进先出法，在库存物资保持一定余额的条件下，帐面的存货计价永远是最初购进的高价，这就造成了存货成本的流转与实物流转的不一致。\r\n其优点是使企业不能随意挑选存货计价，以调整当期利润，缺点是工作量比较繁琐，特别对于存货进出量频繁的企业更是如此。\r\n当物价上涨时，会高估企业当期利润和库存存货价值。反之，会低估企业存货价值和当期利润。\r\n\r\n\r\n2、加权平均法 \r\n加权平均法亦称全月一次加权平均法，是根据起初存货结余和本期收入存货的数量及进价成本，期末一次计算存货的本月加权平均单价，作为计算本期发出存货成本和期末结存价值的单价，以求得本期发出存货成本和结存存货价值的一种方法。 其特点是：所求得的平均数，已包含了长期趋势变动。这种方法适用于前后进价相差幅度不大且月末定期计算和结转销售成本的商品。\r\n\r\n\r\n\r\n优点：只在月末一次计算加权平均单价，比较简单，而且在市场价格上涨或下跌时所计算出来的单位成本平均化，对存货成本的分摊较为折中。\r\n\r\n缺点：不利于核算的及时性；在物价变动幅度较大的情况下，按加权平均单价计算的期末存货价值与现行成本有较大的差异。适合物价变动幅度不大的情况。这种方法平时无法从账上提供发出和结存存货的单价及金额，不利于加强对存货的管理。为解决这一问题，可以采用移动加权平均法或按上月月末计算的平均单位成本计算。\r\n\r\n\r\n\r\n3、移动加权平均法 \r\n移动加权平均法是指每次收货后，立即根据库存存货数量和总成本，计算出新的平均单价或成本的1种方法。 \r\n\r\n\r\n移动加权平均法下库存商品的成本价格根据每次收入类单据自动加权平均；其计算方法是以各次收入数量和金额与各次收入前的数量和金额为基础，计算出移动加权平均单价。其计算公式如下：\r\n\r\n移动加权平均单价= (本次收入前结存商品金额+本次收入商品金额)/(本次收入前结存商品数量+本次收入商品数量 )\r\n\r\n移动加权平均法计算出来的商品成本比较均衡和准确，但计算起来的工作量大，一般适用于经营品种不多、或者前后购进商品的单价相差幅度较大的商品流通类企业。\r\n\r\n以下以一个简单的例子来说明：\r\n\r\n例1：货品A，期初结存数量10，加权价10，金额为100，发生业务如下：\r\n\r\n销售11；采购10，采购价格11；\r\n\r\n成本计算过程如下：\r\n\r\n销售时，期初成本金额为10*10=100；本期购入成本：10*11=110.销售后结存数量：9；\r\n\r\n采购后，结存单价为：（10*10+10*11）/（10+10）=10.5\r\n\r\n新的企业所得税法已经取消了后进先出法，只剩下先进先出法，加权平均法，个别计价法，移动加权平均法包含在加权平均法中。\r\n\r\n\r\n\r\n\r\n4、后进先出法 \r\n后进先出法是假定后收到的存货先发出或后收到的存货先耗用，并根据这种假定的存货流转次序对发出存货和期末存货进行计价的一种方法。 \r\n\r\n5、个别计价法 \r\n个别计价法是以每次(批)收入存货的实际成本作为计算各该次(批)发出存货成本的依据。即：每次(批)存货发出成本=该次(批)存货发出数量X该次(批)存货实际收入的单位成本除上述计价法外，还有计划成本法、毛利率法、售价金额核算法等，但前五种方法属于企业按实际成本计价的存货发出的计价方法。存货期束计价通常是以实际成本确定。\r\n个别计价法的优点：计算发出存货的成本和期末存货的成本比较合理、准确。缺点：实务操作的工作量繁重，困难较大。适用于容易识别、存货品种数量不多、单位成本较高的存货计价。\r\n\r\n\",\"cid\":\"19083679\",\"created_at\":\"2014-02-11 06:02\",\"ctype\":\"blog\",\"title\":\"存货的计价方法\"}CONSOLE# 00_843625FCC33C946EF7F700E7C2F4393C {\"body\":\" \r\n我们要访问一个网站，通常是在游览器里输入这个网站的网址，然后回车，这个时候，DNS服务器会自动把它解析成IP地址，实际上我们是通过IP来访问网站的，网址只不过是助记符罢了。那么在局域网中，以太网设备并不认识IP地址，所以还要将IP地址转换成MAC地址，ARP（Address Resolution Protocol）就是进行这种转换的协议。\r\n \r\n因特网是通过TCP/IP进行信息交换的，所以要上网，必须安装TCP/IP协议，在每台安装了此协议的电脑里，都有一个ARP缓存表，表里的IP地址和MAC地址是一一对应的：\r\n\r\n \r\n \r\n \r\n \r\n \r\n \r\n \r\n \r\n \r\n \r\n \r\n \r\n \r\n \r\n \r\n \r\n \r\n \r\n \r\n \r\n \r\n \r\n\r\nIP地址\r\n 	\r\nMAC地址\r\n\r\n10.6.13.1\r\n 	\r\n00-00-00-00-01\r\n\r\n10.6.13.2\r\n 	\r\n00-00-00-00-02\r\n\r\n10.6.13.3\r\n 	\r\n00-00-00-00-03\r\n\r\n ……\r\n 	\r\n……\r\n\r\n \r\n当10.6.13.2要向10.6.13.1发送信息时，首先在ARP缓存表里查找是否有10.6.13.1的MAC地址，如果有，就直接进行通信，如果没有，就要在网络上发送一个广播信息，询问10.6.13.1的MAC地址，这时，其它电脑并不响应询问，只有10.6.13.1响应，它把自己的MAC地址发送给10.6.13.2，这样，10.6.13.2就知道10.6.13.1的MAC地址了，它们之间就可以进行通信了，同时，10.6.13.2也更新了自己的ARP缓存表，下次通信时就可以直接从中取出10.6.13.1的MAC地址进行通信了。ARP缓存表还采用了老化机制，在一段时间里如果某一行没有使用，就会被删除，这样就大大减小了表的长度，加快了查询速度。\r\n那么什么又是网关呢？\r\n顾名思义，网关(Gateway)就是一个网络连接到另一个网络的“关口”。\r\n按照不同的分类标准，网关也有很多种。TCP/IP协议里的网关是最常用的，在这里我们所讲的“网关”均指TCP/IP协议下的网关。\r\n那么网关到底是什么呢？网关实质上是一个网络通向其他网络的IP地址。比如有网络A和网络B，网络A的IP地址范围为“192.168.1.1~192. 168.1.254”，子网掩码为255.255.255.0；网络B的IP地址范围为“192.168.2.1~192.168.2.254”，子网掩码为255.255.255.0。在没有路由器的情况下，两个网络之间是不能进行TCP/IP通信的，即使是两个网络连接在同一台交换机(或集线器)上，TCP/IP协议也会根据子网掩码(255.255.255.0)判定两个网络中的主机处在不同的网络里。而要实现这两个网络之间的通信，则必须通过网关。如果网络A中的主机发现数据包的目的主机不在本地网络中，就把数据包转发给它自己的网关，再由网关转发给网络B的网关，网络B的网关再转发给网络B的某个主机。网络B向网络A转发数据包的过程也是如此。\r\n所以说，只有设置好网关的IP地址，TCP/IP协议才能实现不同网络之间的相互通信。那么这个IP地址是哪台机器的IP地址呢？网关的IP地址是具有路由功能的设备的IP地址，具有路由功能的设备有路由器、启用了路由协议的服务器(实质上相当于一台路由器)、代理服务器(也相当于一台路由器)。\r\n现在我们来实际操作一下\r\n点击“开始”-“运行”-输入cmd，回车\r\n在命令提示符下输入arp –a\r\n\\r\n如上图，192.168.1.1的物理地址（MAC地址）是00-aa-00-62-c6-09，type是dynamic（这是我绑定后截的图，所以是static）。记下它的MAC，然后输入arp –s 192.168.1.1 00-aa-00-62-c6-09，这样就在你的ARP缓存表里添加了一个192.168.1.1到00-aa-00-62-c6-09的对应关系。\r\n1 在命令提示符中输入\"arp -a\" 这时有可能得到很多IP和它的MAC，但注意了，我们只想得到网关MAC（一般网关是当前网段最开始的那个IP，比如我通过DHCP得到的IP是10.6.13.6，那么网关就是10.6.13.1）\r\n2 输入\"arp -s 网关IP 网关MAC\"\r\n3 输入\"exit\"退出4 搞定---------------------\r\n提示：Arp –d命令还可以删除一个对应关系。\r\n \r\n \",\"cid\":\"1628911\",\"created_at\":\"2007-05-29 01:04\",\"ctype\":\"blog\",\"title\":\"校园网经常掉线解决\"}"), new Text("title")));
//	}

    public Text evaluate(Text str, Text key) {
        output.set("-");
        if (str == null || key == null) {
            return output;
        }
        try {
            JSONObject object = JSONObject.fromObject(str.toString().replaceAll("\r\n|\r|\n", ""));
            if (object.containsKey(key.toString())) {
                output.set(object.get(key.toString()).toString());
            }
        } catch (Exception e) {

        }
        return output;
    }

}
