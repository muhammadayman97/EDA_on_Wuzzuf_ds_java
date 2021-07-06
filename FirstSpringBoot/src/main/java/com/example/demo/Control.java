package com.example.demo;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;
import java.util.LinkedList;
import java.util.List;

import javax.imageio.ImageIO;
import javax.swing.*;
import java.io.File;
import java.io.IOException;
import org.knowm.xchart.SwingWrapper;


public class Control {

    public JFrame draw_pieChart(Dataset<Row> data,String filename) throws IOException {

        PieChart pie = new PieChart(900, 900);
        for (int i = 0; i < 5; i++) {
            // subList and i<in loop> control number of sectors appear in pieChart
            pie.addSeries(String.valueOf(data.collectAsList().subList(0, 5).get(i).get(0)),
                    Integer.parseInt(String.valueOf(data.collectAsList().subList(0, 10).get(i).get(1))));
        }
        BitmapEncoder.saveBitmap(pie, "/home/muhammad/Desktop/FirstSpringBoot/public/data/"+filename, BitmapEncoder.BitmapFormat.PNG);

        return new SwingWrapper<PieChart>(pie).displayChart();


    }

    public JFrame draw_barChart(Dataset<Row> data, String x_axis_title, String bar_plot_title,String filename) throws IOException {

        // Create Chart

        CategoryChart bar = new CategoryChartBuilder().width(800).height(600).title(bar_plot_title).xAxisTitle(x_axis_title).yAxisTitle("Frequency").build();
        bar.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        bar.getStyler().setHasAnnotations(true);
        bar.getStyler().setXAxisLabelRotation(70);
        List<Integer> x = new LinkedList<>();
        List<String> s = new LinkedList<>();
        for (int i = 0; i <= 10; i++) {
            s.add(String.valueOf(data.collectAsList().get(i).get(0)));
            x.add(Integer.parseInt(String.valueOf(data.collectAsList().get(i).get(1))));
        }
        bar.addSeries("test", s, x);

        BitmapEncoder.saveBitmap(bar, "/home/muhammad/Desktop/FirstSpringBoot/public/data/"+filename, BitmapEncoder.BitmapFormat.PNG);

        return new SwingWrapper<CategoryChart>(bar).displayChart();

    }
}

