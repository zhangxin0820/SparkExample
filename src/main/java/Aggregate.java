import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by zhangxin on 2017/8/30.
 * Time : 21:25
 */
public class Aggregate implements Serializable {

    public int total;
    public int num;

    public Aggregate(int total, int num) {
        this.total = total;
        this.num = num;
    }

    public double avg() {
        return total / (double) num;
    }

    static Function2<Aggregate, Integer, Aggregate> addAndCount =
            new Function2<Aggregate, Integer, Aggregate>() {
                public Aggregate call(Aggregate a, Integer x) throws Exception {
                    a.total += x;
                    a.num += 1;
                    return a;
                }
            };
    static Function2<Aggregate, Aggregate, Aggregate> combine =
            new Function2<Aggregate, Aggregate, Aggregate>() {
                public Aggregate call(Aggregate a, Aggregate b) throws Exception {
                    a.total += b.total;
                    a.num += b.num;
                    return a;
                }
            };

    public static void main(String args[]) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Aggregate intial = new Aggregate(0, 0);
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6));
        Aggregate result = rdd.aggregate(intial, addAndCount, combine);
        System.out.println(result.avg());

        sc.close();
    }
}
