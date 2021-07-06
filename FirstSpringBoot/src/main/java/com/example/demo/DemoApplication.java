package com.example.demo;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.sql.Encoders;
import org.knowm.xchart.BitmapEncoder;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoders;
import org.knowm.xchart.PieChart;
import org.knowm.xchart.SwingWrapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.GetMapping;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;


import java.io.IOException;

import static org.apache.spark.sql.functions.*;

@SpringBootApplication
public class DemoApplication {
    public static void main(String[] args) throws IOException {
        SpringApplication.run (DemoApplication.class, args);

    //1. Read data set and convert it to dataframe or Spark RDD and display some from it.

        Logger.getLogger("org").setLevel(org.apache.log4j.Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
      //  Logger.getLogger("org").setLevel(Level.WARNING);
        final SparkSession session = SparkSession.builder ().appName ("Spark CSV Analysis Demo").master ("local[*]")
                .getOrCreate ();
        final DataFrameReader dataFrameReader = session.read ();


        Dataset<Row> dataset = dataFrameReader.option("header" , "true")
                .csv("/home/muhammad/Desktop/FirstSpringBoot/src/main/resources/Wuzzuf_Jobs.csv");

        Dataset<Row> responseWithSelectedColumns = dataset.select(
                col("title"),
                col("company"),
                col("location"),
                col("type"),
                col("level"),
                col("yearsExp"),
                col("country"),
                col("skills"));

        // Object Of DAO Class
        Dataset<DAO> DAO_Object = responseWithSelectedColumns.as(Encoders.bean(DAO.class));


        //2. Clean the data (null, duplications)

            // Remove Any Duplicate From DataSet
        dataset = dataset.dropDuplicates();

            // Check For Any null Values or Missing
        System.out.println(" Check For Null " + dataset.isEmpty());

            //display some from dataset

        System.out.println("=== Print 20 records of responses table ===");
        dataset.show(20);


        //3. Display structure and summary of the data.

        System.out.println("=== Summary of Data ===");
        dataset.describe().show();

        System.out.println("=== Structure of Data ===");
        dataset.printSchema();

        //4. Count the jobs for each company and display that in order (What are the most demanding companies for jobs?)

        Dataset<Row> jobs_for_company = DAO_Object.filter((FilterFunction<DAO>) response -> response.getTitle() != null)
                .groupBy("company")
                .count()
                .orderBy("company")
                .sort(col("count").desc()).toDF();
            // Display The Count the jobs for each company
        jobs_for_company.show();

        //5. Show step 4 in a pie chart

        //to enable to async tasks at the same time where the server works asynchronously and so the image rendering process
        System.setProperty("java.awt.headless", "false");
        Control con1 = new Control();
        con1.draw_pieChart(jobs_for_company, "jobs for each company");

        //6. Find out What are it the most popular job titles?
        Dataset<Row> pop_titles = DAO_Object.filter((FilterFunction<DAO>) response -> response.getTitle() != null)
                .groupBy("Title")
                .count()
                .orderBy("Title")
                .sort(col("count").desc()).toDF();

        System.out.println("=== count pop_titles ===");
        pop_titles.show();



        //7. Show step 6 in bar chart

        Control con2 = new Control();

        con2.draw_barChart(pop_titles, "job titles","the most popular job titles", "job titles");



        //8. Find out the most popular areas?
        Dataset<Row> pop_areas = DAO_Object.filter((FilterFunction<DAO>) response -> response.getLocation() != null)
                .groupBy("Location")
                .count()
                .orderBy("Location")
                .sort(col("count").desc()).toDF();


        System.out.println("=== count pop_areas ===");
        pop_areas.show();


        //9. Show step 8 in bar chart

        Control con3 = new Control();
        con3.draw_barChart(pop_areas, "locations","the most popular locations", "locations");



        //10. Print skills one by one and how many each repeated and order the output to find out the most important skills required?

        System.out.println("=== count of popular skill ===");
        Dataset<Row> skills = DAO_Object.select("Skills").toDF("line");
        skills.select(explode(split(col("line"),","))
                .as("word"))
                .groupBy("word")
                .count()
                .sort(col("count").desc()).toDF()
                .show();



        //11. Factorize the YearsExp feature and convert it to numbers in new col. (Bounce )

        Dataset convert = dataset.select("title","company" , "location" , "type" , "level" ,"yearsExp", "country" ,"skills")
                .where("YearsExp != \"null Yrs of Exp\" ");

        StringIndexerModel indexer = new StringIndexer()
                .setInputCol("yearsExp")
                .setOutputCol("YearsExpConverted")
                .fit(convert);
        Dataset<Row> indexed = indexer.transform(convert);

        System.out.println("=== Factorized YearsExp column===");
        indexed.show(50);


        // Convert Jobs for company to JSON
        System.out.println(jobs_for_company.toJSON().collectAsList());



        //12. Apply K-means for job title and companies (Bounce )

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

                return Vectors.dense(values);}
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


    }


}