/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package usYoutubeVidPkg;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import static usYoutubeVidPkg.YoutubeTitleWordCount.tagsFn;
import static usYoutubeVidPkg.YoutubeTitleWordCount.titlesFn;

/**
 *
 * @author SaiF El-deen
 */
public class MainClass {
   
    public static void main(String[] args) throws IOException {
        
        Logger.getLogger ("org").setLevel (Level.ERROR);
        // CREATE SPARK CONTEXT
        SparkConf conf = new SparkConf ().setAppName ("wordCounts").setMaster ("local[3]");
        JavaSparkContext sparkContext = new JavaSparkContext (conf);
        // LOAD DATASETS
        JavaRDD<String> videos = sparkContext.textFile ("src/main/resources/USvideos.csv");
 
        // PRINTING
        System.out.println("titles =");
        titlesFn(videos);
        System.out.println("tags =");
        tagsFn(videos);
    }

}

