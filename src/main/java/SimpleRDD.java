import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Created by zhangxin on 2017/8/30.
 * Time : 21:31
 */
public class SimpleRDD implements Serializable {
    
    public void simpleOperation() {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("SimpleRDD");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 3), 2);

        /*System.out.println("rdd collect:" + rdd.collect());
        System.out.println("rdd count:" + rdd.count());
        System.out.println("rdd countByValue:" + rdd.countByValue());
        System.out.println("rdd take:" + rdd.take(2));
        System.out.println("rdd top:" + rdd.top(2));
        System.out.println("rdd takeOrdered:" + rdd.takeOrdered(2));*/
        /*System.out.println("rdd reduce:" + rdd.reduce(new Function2<Integer, Integer, Integer>() {

            public Integer call(Integer a, Integer b) throws Exception {

                return a + b;
            }
        }));
        System.out.println("rdd fold:" + rdd.fold(0, new Function2<Integer, Integer, Integer>() {

            public Integer call(Integer a, Integer b) throws Exception {

                return a + b;
            }
        }));*/




        System.out.println("rdd aggregate test");

        List<Integer> data = Arrays.asList(5, 1, 1, 4, 4, 2, 6);

        JavaRDD<Integer> javaRDD = sc.parallelize(data, 2);

        Integer value = javaRDD.aggregate(3, new Function2<Integer, Integer, Integer>() {

            public Integer call(Integer x, Integer y) throws Exception {

                return Math.max(x, y);
            }
        }, new Function2<Integer, Integer, Integer>() {

            int i = 0;

            public Integer call(Integer v1, Integer v2) throws Exception {

                System.out.println("comb~~~~~~~~~i~~~~~~~~~~~~~~~~~~~"+i++);

                System.out.println("comb~~~~~~~~~v1~~~~~~~~~~~~~~~~~~~" + v1);

                System.out.println("comb~~~~~~~~~v2~~~~~~~~~~~~~~~~~~~" + v2);

                return v1 * v2;
            }
        });

        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"+value);

        System.out.println("foreach");

        rdd.foreach(new VoidFunction<Integer>() {

            public void call(Integer t) throws Exception {

                System.out.println(t);
            }
        });

        sc.close();

    }
}
