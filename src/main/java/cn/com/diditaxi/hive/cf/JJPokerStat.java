package cn.com.diditaxi.hive.cf;

/**
 * @author heqi
 * @date 2014-5-30
 * note: for count session
 */
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class JJPokerStat extends UDAF {
	public static class JJCountUDAFEvaluator implements UDAFEvaluator {
		public static final Logger log = Logger.getLogger(JJPokerStat.class);

		public static class PartialResult {
			int total = 0;
			int win = 0;
			int fold = 0;
			int allin = 0;
		}

		private PartialResult result;

		public void init() {
			result = new PartialResult();
		}

		public boolean calwin(int rbet, String chipwon) {
			if (chipwon.equalsIgnoreCase("NULL")) {
				return false;
			}
			String[] cw = chipwon.split("\\|");
			int chipwons = 0;
			// log.info("calwin:"+chipwon);
			for (String v : cw) {
				String[] c = v.split(":");
				// log.info("calwin:v "+v+",c.length:"+c.length);
				if (c.length > 1) {
					chipwons += Integer.parseInt(c[1]);
				}
			}
			// log.info("calwin:chipwons:"+chipwons+",rbet:"+rbet);
			if (chipwons > rbet) {
				return true;
			}
			return false;
		}

		public boolean iterate(IntWritable rbet, Text chipwon, IntWritable f,
				IntWritable a) {
			if (rbet == null || chipwon == null || f == null || a == null) {
				return true;
			}
			boolean win = calwin(rbet.get(), chipwon.toString());
			if (result == null) {
				result = new PartialResult();
			}
			result.total++;
			if (win) {
				result.win++;
			}
			int v = f.get();
			if (v >= 1) {
				result.fold++;
			}
			v = a.get();
			if (v >= 1) {
				result.allin++;
			}
			return true;
		}

		public PartialResult terminatePartial() {
			return result;
		}

		public boolean merge(PartialResult other) {
			if (other == null) {
				return true;
			}
			result.total += other.total;
			result.win += other.win;
			result.fold += other.fold;
			result.allin += other.allin;
			return true;
		}

		public Text terminate() {
			if (result == null) {
				return new Text("0\t0\t0\t0");
			}
			String s = "" + result.total + "\t" + result.win + "\t"
					+ result.fold + "\t" + result.allin;
			return new Text(s);
		}
	}
}