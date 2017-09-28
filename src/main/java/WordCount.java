import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by zhangxin on 2017/8/29.
 * Time : 13:43
 */
public class WordCount implements Serializable {

    public void wordCount() {

        /**
         * 对于所有的spark程序所言，要进行所有的操作，首先要创建一个spark上下文。
         * 在创建上下文的过程中，程序会向集群申请资源及构建相应的运行环境。
         * 设置spark应用程序名称
         * 创建的 sc 唯一需要的参数就是 sparkConf，它是一组 K-V 属性对。
         */
        SparkConf sparkConf = new SparkConf().setAppName("Spark WordCount").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        sc.setLogLevel("warn");



        /**
         * 利用textFile接口从文件系统中读入指定的文件，返回一个RDD实例对象。
         * RDD的初始创建都是由SparkContext来负责的，将内存中的集合或者外部文件系统作为输入源。
         * RDD：弹性分布式数据集，即一个 RDD 代表一个被分区的只读数据集。一个 RDD 的生成只有两种途径，
         * 一是来自于内存集合和外部存储系统，另一种是通过转换操作来自于其他 RDD，比如 Map、Filter、Join，等等。
         * textFile()方法可将本地文件或HDFS文件转换成RDD，读取本地文件需要各节点上都存在，或者通过网络共享该文件读取一行
         */
        final JavaRDD<String> lines = sc.textFile("/Users/zhangxin/spark-2.2.0-bin-hadoop2.7/README.md");



        /**
         * new FlatMapFunction<String, String>两个string分别代表输入和输出类型。
         * Override的call方法需要自己实现一个转换的方法，并返回一个Iterable的结构。
         *
         * flatmap属于一类非常常用的spark函数，简单的说作用就是将一条rdd数据使用你定义的函数给分解成多条rdd数据。
         * 例如，当前状态下，lines这个rdd类型的变量中，每一条数据都是一行String，我们现在想把他拆分成1个个的词的话，
         * 可以这样写：
         *
         * flatMap与map的区别是，对每个输入，flatMap会生成一个或多个的输出，而map只是生成单一的输出，
         * 用空格分割各个单词,输入一行,输出多个对象,所以用flatMap
         */
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            public Iterator<String> call(String line) throws Exception {

                line = line.replaceAll("[,]", "").replaceAll("[.]", "");

                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        /*List<String> temp = words.collect();
        System.out.println(temp);*/

        /**
         * map 键值对 ，类似于MR的map方法。
         * pairFunction<T,K,V>: T:输入类型；K,V：输出键值对。
         * 表示输入类型为T,生成的key-value对中的key类型为k,value类型为v,对本例,T=String, K=String, V=Integer(计数)。
         * 需要重写call方法实现转换
         */
        final JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

            //Tuple2为scala中的一个对象,call方法的输入参数为T,即输入一个单词word,新的Tuple2对象的key为这个单词,计数为1。
            public Tuple2<String, Integer> call(String word) throws Exception {

                return new Tuple2<String, Integer>(word, 1);
            }
        });



        /**
         * 调用reduceByKey方法,按key值进行reduce。
         * reduceByKey方法，类似于MR的reduce。
         * 要求被操作的数据（即下面实例中的pairs）是KV键值对形式，该方法会按照key相同的进行聚合，在两两运算：
         * 例如pairs有<"one", 1>, <"one", 1>,会根据"one"将相同的pair单词个数进行统计,输入为Integer,输出也为Integer，
         * 输出<"one", 2>。
         */
        JavaPairRDD<String, Integer> wordsCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

            public Integer call(Integer v1, Integer v2) throws Exception {

                return v1 + v2;
            }
        });



        /**
         * collect方法用于将spark的RDD类型转化为我们熟知的java常见类型
         */
        /*List<Tuple2<String, Integer>> output = wordsCount.collect();

        for (Tuple2<String, Integer> tuple : output) {

            System.out.println(tuple._1 + ":" + tuple._2);
        }*/

        /**
         * 或者这么写：
         */
        wordsCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {

            public void call(Tuple2<String, Integer> pairs) throws Exception {

                System.out.println(pairs._1 + ":" + pairs._2);
            }
        });

        sc.close();
    }
}
