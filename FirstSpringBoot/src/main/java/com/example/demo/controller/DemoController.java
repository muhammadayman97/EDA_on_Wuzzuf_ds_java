package com.example.demo.controller;

import com.example.demo.DAO;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.apache.spark.sql.functions.*;

@RestController
@RequestMapping("data")
public class DemoController {


    // Start Spark Session
    public DataFrameReader getFrameReader() {
        final SparkSession session = SparkSession.builder().appName("Spark CSV Analysis Demo").master("local[*]")
                .getOrCreate();
        return session.read();

    }

    // Read dataset
    public Dataset<Row> getDataSet() {
        Dataset<Row> dataset = getFrameReader().option("header", "true")
                .csv("/home/muhammad/Desktop/FirstSpringBoot/src/main/resources/Wuzzuf_Jobs.csv");
        Dataset<Row> d = dataset.select(
                col("title"),
                col("company"),
                col("location"),
                col("type"),
                col("level"),
                col("yearsExp"),
                col("country"),
                col("skills"));
        d = dataset.dropDuplicates();

        return d;
    }


    @GetMapping("/test")
    public String test() {
        return "Java Sucks";
    }

    //2. Show number of records of dataset
    @GetMapping("/getNofRecords")
    public List<String> getNOfRecords(@RequestParam(value = "number", required = true) int n) {
        Dataset<Row> dataset = getDataSet();
        return dataset.toJSON().collectAsList().subList(0, n);
    }

    //3. Display structure and summary of the data.
    @GetMapping("/Summary")
    public List<String> getSummary() {
        Dataset<Row> dataset = getDataSet();

        return dataset.describe().toJSON().collectAsList();

    }

    //3.
    @GetMapping("/getSchema")
    public StructType getSchema() {
        Dataset<Row> dataset = getDataSet();
        return dataset.schema();
    }

    //4. Count the jobs for each company
    @GetMapping("/getJobsForCompany")
    public List<String> getJobsForCompany() {
        Dataset<DAO> DAO_Object = getDataSet().as(Encoders.bean(DAO.class));
        Dataset<Row> jobs_for_company = DAO_Object.filter((FilterFunction<DAO>) response -> response.getTitle() != null)
                .groupBy("company")
                .count()
                .orderBy("company")
                .sort(col("count").desc()).toDF();


        return jobs_for_company.toJSON().collectAsList();
    }

    //5. PieChart Stats for the jobs for company
    @GetMapping("/getPieStat")
    public HashMap<String, String> getPieStat(@RequestParam(value = "number", required = true) int n) {

        Dataset<DAO> DAO_Object = getDataSet().as(Encoders.bean(DAO.class));
        Dataset<Row> jobs_for_company = DAO_Object.filter((FilterFunction<DAO>) response -> response.getTitle() != null)
                .groupBy("company")
                .count()
                .orderBy("company")
                .sort(col("count").desc()).toDF();
        HashMap<String, String> dataMap = new HashMap<>();
        for (int i = 0; i < n; i++) {
            dataMap.put(String.valueOf(jobs_for_company.collectAsList().get(i).get(0)), String.valueOf(jobs_for_company.collectAsList().get(i).get(1)));
        }

        return dataMap;
    }

    //6. Show the PieChart
    @GetMapping("/getPie")
    public String getPie() {

        String img = "<!DOCTYPE html>\n" +
                "<html>\n" +
                "<body>\n" +
                "\n" +
                "<p>Pie Company Plot</p>\n" +
                "\n" +
                "<img src=\"jobs for each company.png\">" +
                "\n" +
                "</body>\n" +
                "</html>" +
                "";
        return img;

    }


    // 7. Find out What are it the most popular job titles
    @GetMapping("/getTitle")
    public List<String> getTitle(@RequestParam(value = "number", required = true) int n) {
        Dataset<DAO> DAO_Object = getDataSet().as(Encoders.bean(DAO.class));
        Dataset<Row> pop_titles = DAO_Object.filter((FilterFunction<DAO>) response -> response.getTitle() != null)
                .groupBy("Title")
                .count()
                .orderBy("Title")
                .sort(col("count").desc()).toDF();
        return pop_titles.toJSON().collectAsList().subList(0, n);

    }

    //8. BarChart Stats for the title of jobs
    @GetMapping("/getBar1Stat")
    public HashMap<String, String> getBar1Stat(@RequestParam(value = "number", required = true) int n) {

        Dataset<DAO> DAO_Object = getDataSet().as(Encoders.bean(DAO.class));
        Dataset<Row> pop_titles = DAO_Object.filter((FilterFunction<DAO>) response -> response.getTitle() != null)
                .groupBy("Title")
                .count()
                .orderBy("Title")
                .sort(col("count").desc()).toDF();

        HashMap<String, String> dataMap = new HashMap<>();
        for (int i = 0; i < n; i++) {
            dataMap.put(String.valueOf(pop_titles.collectAsList().get(i).get(0)), String.valueOf(pop_titles.collectAsList().get(i).get(1)));
        }

        return dataMap;
    }


    //9. Show the BarChart for titles
    @GetMapping("/getBarTitle")
    public String getBar() {

        String img = "<!DOCTYPE html>\n" +
                "<html>\n" +
                "<body>\n" +
                "\n" +
                "<p>Bar Title Plot</p>\n" +
                "\n" +
                "<img src=\"job titles.png\">" +
                "\n" +
                "</body>\n" +
                "</html>" +
                "";
        return img;

    }


    //10. Find out the most popular Locations
    @GetMapping("/getLocation")
    public List<String> getLocation(@RequestParam(value = "number", required = true) int n) {
        Dataset<DAO> DAO_Object = getDataSet().as(Encoders.bean(DAO.class));
        Dataset<Row> pop_titles = DAO_Object.filter((FilterFunction<DAO>) response -> response.getTitle() != null)
                .groupBy("Location")
                .count()
                .orderBy("Location")
                .sort(col("count").desc()).toDF();
        return pop_titles.toJSON().collectAsList().subList(0, n);

    }

    //11. BarChart Stats for Location of jobs
    @GetMapping("/getBar2Stat")
    public HashMap<String, String> getBar2Stat(@RequestParam(value = "number", required = true) int n) {

        Dataset<DAO> DAO_Object = getDataSet().as(Encoders.bean(DAO.class));
        Dataset<Row> pop_titles = DAO_Object.filter((FilterFunction<DAO>) response -> response.getTitle() != null)
                .groupBy("Location")
                .count()
                .orderBy("Location")
                .sort(col("count").desc()).toDF();

        HashMap<String, String> dataMap = new HashMap<>();
        for (int i = 0; i < n; i++) {
            dataMap.put(String.valueOf(pop_titles.collectAsList().get(i).get(0)), String.valueOf(pop_titles.collectAsList().get(i).get(1)));
        }

        return dataMap;
    }

    //12. Show BarChart of the Jobs Location
    @GetMapping("/getBarLocation")
    public String getBarLoc() {

        String img = "<!DOCTYPE html>\n" +
                "<html>\n" +
                "<body>\n" +
                "\n" +
                "<p>Bar Location Plot</p>\n" +
                "\n" +
                "<img src=\"locations.png\">" +
                "\n" +
                "</body>\n" +
                "</html>" +
                "";
        return img;

    }


    //13. Get skills one by one and how many each repeated
    @GetMapping("/Skills")
    public List<String> getSkills() {
        Dataset<DAO> DAO_Object = getDataSet().as(Encoders.bean(DAO.class));
        Dataset<Row> skills = DAO_Object.select("Skills").toDF("line");
        skills.select(explode(split(col("line"), ","))
                .as("word"))
                .groupBy("word")
                .count()
                .sort(col("count").desc()).toDF();

        return skills.toJSON().collectAsList();
    }


    //14. Factorize the YearsExp feature and convert it to numbers in new col.
    @GetMapping("/YearsExp")
    public List<String> getExp() {

        Dataset convert = getDataSet().select("title", "company", "location", "type", "level", "yearsExp", "country", "skills")
                .where("YearsExp != \"null Yrs of Exp\" ");


        StringIndexerModel indexer = new StringIndexer()
                .setInputCol("yearsExp")
                .setOutputCol("YearsExpConverted")
                .fit(convert);
        Dataset<Row> indexed = indexer.transform(convert);

        return indexed.toJSON().collectAsList();

    }


    @GetMapping("/kmeans")
    public List<String> getKmeans() {

        final SparkSession session = SparkSession.builder().appName("Spark CSV Analysis Demo").master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset = getFrameReader().option("header", "true")
                .csv("/home/muhammad/Desktop/FirstSpringBoot/src/main/resources/Wuzzuf_Jobs.csv");
        Dataset Kmeans_data = dataset.select("title", "company", "location", "type", "level", "yearsExp", "country", "skills");

        StringIndexerModel indexer_kmeans = new StringIndexer()
                .setInputCols(new String[]{"title", "company", "location", "type", "level", "yearsExp", "country", "skills"})
                .setOutputCols(new String[]{"title_new", "company_new", "location_new", "type_new", "level_new", "yearsExp_new", "country_new", "skills_new"})
                .fit(Kmeans_data);
        Dataset<Row> indexed_kmeans = indexer_kmeans.transform(Kmeans_data);
        Dataset new_dataset = indexed_kmeans.select("title_new", "company_new");
        System.out.println("=== Factorized 'company' and 'title' columns===");
        new_dataset.show(5);


        SparkContext conf = session.sparkContext();
        //SparkConf conf = new SparkConf().setAppName("SparkKMeansExample").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        //JavaRDD<String> data = jsc.textFile("/home/muhammad/Desktop/SparkProject-main/JavaFinalProject/src/main/resources/Wuzzuf_Jobs.csv");
        JavaRDD<Vector> parsedData = new_dataset.javaRDD().map(new Function<Row, Vector>() {
            public Vector call(Row s) {
                double[] values = new double[2];
                values[0] = Double.parseDouble(s.get(0).toString());
                values[1] = Double.parseDouble(s.get(1).toString());

                return Vectors.dense(values);
            }
        });
        parsedData.cache();

        int numClusters = 4;
        int numIterations = 20;
        KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);

        System.out.println("Cluster centers:");
        for (Vector center : clusters.clusterCenters()) {
            System.out.println(" " + center);
        }


        double WSSSE = clusters.computeCost(parsedData.rdd());
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);


        String out = "<html><body>";
        out += "<h1>" + "calculate" + "<h1><br><p>";

        for (Vector str : clusters.clusterCenters()) {
            out += str.toString();
            out += "<br>";

        }
        out += "<br>" + "cal final " + WSSSE + "<br>";
        out += "</p></body></html>";
        return Collections.singletonList(out);
    }



}
