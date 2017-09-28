

/**
 * Created by zhangxin on 2017/8/29.
 * Time : 12:57
 */
public class SparkTest {

    public static void main(String[] args) {

        /**
         * 由于flatap等算子(map, filter等)内部使用了外部定义的变量 需要对类进行序列化
         */
        /*WordCount wc = new WordCount();

        wc.wordCount();

        test1 test1 = new test1();
        test1.test();*/

        SimpleRDD simpleRDD = new SimpleRDD();

        simpleRDD.simpleOperation();
    }

}
