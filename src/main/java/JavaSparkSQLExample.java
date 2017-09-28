import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 * Created by zhangxin on 2017/9/1.
 * Time : 19:48
 */
public class JavaSparkSQLExample {

    public static class Person implements Serializable {

        private String name;

        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }

    public static void main(String[] args) throws AnalysisException {

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[*]")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        /*runBasicDataFrameExample(spark);

        runDatasetCreationExample(spark);

        runInferSchemaExample(spark);*/

        runProgrammaticSchemaExample(spark);

        spark.stop();
    }

    private static void runBasicDataFrameExample(SparkSession spark) throws AnalysisException {

        Dataset<Row> df = spark.read().json("/Users/zhangxin/spark-2.2.0-bin-hadoop2.7/examples/src/main/resources/people.json");

        df.show();

        df.printSchema();
        // root
        // |-- age: long (nullable = true)
        // |-- name: string (nullable = true)

        df.select("name").show();

        df.select(col("name"), col("age").plus(1)).show();

        df.filter(col("age").gt(21)).show();

        df.groupBy("age").count().show();

        df.createOrReplaceTempView("people");

        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
        sqlDF.show();

        df.createGlobalTempView("people");

        // 全局临时视图与系统保留数据库global_temp绑定
        spark.sql("SELECT * FROM global_temp.people").show();

        // 全局临时视图是跨session的
        spark.newSession().sql("SELECT * FROM global_temp.people").show();
    }

    private static void runDatasetCreationExample(SparkSession spark) {

        Person person = new Person();
        person.setName("Andy");
        person.setAge(32);
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = spark.createDataset(
                Collections.singletonList(person),
                personEncoder
        );
        javaBeanDS.show();

        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
        Dataset<Integer> transformedDS = primitiveDS.map(new MapFunction<Integer, Integer>() {

            public Integer call(Integer value) throws Exception {
                return value + 1;
            }
        }, integerEncoder);
        transformedDS.collect();

        String path = "/Users/zhangxin/spark-2.2.0-bin-hadoop2.7/examples/src/main/resources/people.json";
        Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
        peopleDS.show();
    }

    private static void runInferSchemaExample(SparkSession spark) {

        JavaRDD<Person> peopleRDD = spark.read().
                textFile("/Users/zhangxin/spark-2.2.0-bin-hadoop2.7/examples/src/main/resources/people.txt")
                .javaRDD()
                .map(new Function<String, Person>() {
                    public Person call(String line) throws Exception {
                        String[] parts = line.split(",");
                        Person person = new Person();
                        person.setName(parts[0]);
                        person.setAge(Integer.parseInt(parts[1].trim()));
                        return person;
                    }
                });

        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);

        peopleDF.createOrReplaceTempView("people");

        Dataset<Row> teenagersDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");

        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(new MapFunction<Row, String>() {
            public String call(Row row) throws Exception {
                return "Name:" + row.getString(0);
            }
        }, stringEncoder);

        teenagerNamesByIndexDF.show();

        Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(new MapFunction<Row, String>() {
            public String call(Row row) throws Exception {
                return "Name:" + row.<String>getAs("name");
            }
        }, stringEncoder);

        teenagerNamesByFieldDF.show();
    }

    private static void runProgrammaticSchemaExample(SparkSession spark) {

        JavaRDD<String> peopleRDD = spark.sparkContext()
                .textFile("/Users/zhangxin/spark-2.2.0-bin-hadoop2.7/examples/src/main/resources/people.txt", 1)
                .toJavaRDD();

        String schemaString = "name age";

        List<StructField> fields = new ArrayList<StructField>();

        for (String fieldName : schemaString.split(" ")) {

            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);

            fields.add(field);
        }

        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowRDD = peopleRDD.map(new Function<String, Row>() {
            public Row call(String record) throws Exception {
                String[] attributes = record.split(",");
                return RowFactory.create(attributes[0], attributes[1].trim());
            }
        });

        Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);

        peopleDataFrame.createOrReplaceTempView("people");

        Dataset<Row> results = spark.sql("SELECT name FROM people");

        List<Row> listRow = results.javaRDD().collect();//toJavaRDD()和javaRDD()一样？
        for (Row row : listRow) {
            System.out.println("Name:" + row.getString(0));
        }

        Dataset<String> namesDS = results.map(new MapFunction<Row, String>() {
            public String call(Row row) throws Exception {
                return "Name:" + row.<String>getAs("name");
            }
        }, Encoders.STRING());

        namesDS.show();
    }
}
