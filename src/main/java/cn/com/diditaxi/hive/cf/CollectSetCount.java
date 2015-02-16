package cn.com.diditaxi.hive.cf;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.Text;

import java.util.Map;

/**
 * 9/14/13 WilliamZhu(allwefantasy@gmail.com)
 */
@Description(
        name = "collect_set_count",
        value = "_FUNC_(key,kvvalue,sep,vSep,kvSep) - 聚合参数。group by时将第二个重复的列拼接并且计数",
        extended = "Example:\n" +
                "  > SELECT collect_set(distinct j,url,'\t','\t','::') FROM authors  group by j\n" +
                "key: 聚合的列\n" +
                "kvvalue: 需要被拼装的列\n" +
                "sep:  key 和 kvvalue的分隔符\n" +
                "vSep:  kvvalue 之间的分隔符\n" +
                "kvSep:  kvvalue 值与计数的分隔符，如果该值为 -| 则不会进行计数"
)
public class CollectSetCount extends UDAF {
    static final Log LOG = LogFactory.getLog(CollectSetCount.class.getName());

    public static class CollectSetData {
        Map<Text, Map<Text, Integer>> collect;
        Text output;
        Text vSep;
        Text vKVSep;
        Text sep;
    }

    public static class CollectSetEvaluator implements UDAFEvaluator {
        private CollectSetData collectSetData;

        public CollectSetEvaluator() {
            super();
            collectSetData = new CollectSetData();
            init();
        }

        @Override
        public void init() {
            collectSetData.collect = Maps.newHashMap();
            collectSetData.output = new Text();
            collectSetData.vKVSep = new Text(":");
            collectSetData.vSep = new Text("\t");
            collectSetData.sep = new Text("\t");
        }


        public boolean iterate(Text o, Text o2, Text sep, Text vSep, Text vKVSep) {

            if (o == null || o2 == null) return false;

            collectSetData.vKVSep = vKVSep;
            collectSetData.vSep = vSep;
            collectSetData.sep = sep;

            if (!collectSetData.collect.containsKey(o.toString())) {
                Map<Text, Integer> temp = Maps.newHashMap();
                collectSetData.collect.put(o, temp);
            }
            increment(collectSetData.collect.get(o), o2);
            return true;
        }

        public Map<Text, Map<Text, Integer>> terminatePartial() {
            return collectSetData.collect;
        }

        public boolean merge(Map<Text, Map<Text, Integer>> o) {
            if (o != null) {
                Map<Text, Map<Text, Integer>> temp = collectSetData.collect;
                for (Map.Entry<Text, Map<Text, Integer>> entry : temp.entrySet()) {
                    if (temp.containsKey(entry.getKey())) {
                        Map<Text, Integer> temp1 = temp.get(entry.getKey());
                        Map<Text, Integer> temp2 = entry.getValue();
                        for (Map.Entry<Text, Integer> entry1 : temp2.entrySet()) {
                            if (temp1.containsKey(entry1.getKey())) {
                                temp1.put(entry1.getKey(), temp1.get(entry1.getKey()) + entry1.getValue());
                            } else {
                                temp1.put(entry1.getKey(), entry1.getValue());
                            }
                        }
                    } else {
                        temp.put(entry.getKey(), entry.getValue());
                    }
                }
                return false;
            }
            return true;
        }

        public Text terminate() {
            if (collectSetData.collect.size() == 0) return null;
            Map.Entry<Text, Map<Text, Integer>> entry = collectSetData.collect.entrySet().iterator().next();
            collectSetData.output.set(entry.getKey() + collectSetData.sep.toString() +
                    (collectSetData.vKVSep.toString().equals("-|") ?
                            Joiner.on(collectSetData.vSep.toString()).join(entry.getValue().keySet())
                            : Joiner.on(collectSetData.vSep.toString()).withKeyValueSeparator(collectSetData.vKVSep.toString()).join(entry.getValue())
                    )
            );
            return collectSetData.output;

        }

        private void increment(Map<Text, Integer> temp, Text o2) {
            if (temp.containsKey(o2)) {
                temp.put(o2, temp.get(o2) + 1);
            } else {
                temp.put(o2, 1);
            }
        }

    }
}
