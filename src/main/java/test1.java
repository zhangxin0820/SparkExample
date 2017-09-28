import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

/**
 * Created by zhangxin on 2017/8/29.
 * Time : 14:01
 */
public class test1 {

    public void test() {

        SparkConf sparkConf = new SparkConf().setAppName("test1").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sc.textFile("/Users/zhangxin/spark-2.2.0-bin-hadoop2.7/README.md");

        List<String> line = lines.collect();

        for (String val : line) {

            System.out.println(val);
        }

        sc.close();
    }
}
