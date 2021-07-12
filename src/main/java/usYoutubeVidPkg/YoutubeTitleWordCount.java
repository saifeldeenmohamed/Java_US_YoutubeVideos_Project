/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package usYoutubeVidPkg;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

/**
 *
 * @author SaiF El-deen
 */
public class YoutubeTitleWordCount {
    
    private static final String COMMA_DELIMITER = ",";
    
    public static String extractTitle(String videoLine) {
        try {
            return videoLine.split (COMMA_DELIMITER)[2];
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
    }
    public static String extractTag(String videoLine) {
        try {
            return videoLine.split (COMMA_DELIMITER)[6];
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
    }
    
    public static void titlesFn(JavaRDD<String> videos) {

    //-----------------------------------Exctact title ---------------------------
        JavaRDD<String> titles = videos
                                        .map (YoutubeTitleWordCount::extractTitle)
                                        .filter (StringUtils::isNotBlank);
        
        
       // JavaRDD<String>
        JavaRDD<String> words = titles.flatMap (title -> Arrays.asList (title
                                                                            .toLowerCase ()
                                                                            .trim ()
                                                                            .replaceAll ("\\p{Punct}", " ")
                                                                            .split (" ")).iterator ());
        System.out.println(words.toString ());
        // COUNTING
        Map<String, Long> wordCounts = words.countByValue ();
        List<Map.Entry> sorted = wordCounts.entrySet ().stream ()
                                                       .sorted (Map.Entry.comparingByValue ())
                                                       .collect (Collectors.toList ());
        // DISPLAY
        for (Map.Entry<String, Long> entry : sorted) 
        {
            System.out.println (entry.getKey () + " : " + entry.getValue ());
        }
    }
    
    
    public static void tagsFn(JavaRDD<String> videos) {
        //-----------------------------------Exctact tag ---------------------------
        JavaRDD<String> tags = videos
                                    .map (YoutubeTitleWordCount::extractTag)
                                    .filter (StringUtils::isNotBlank);
        
        // JavaRDD<String>
        JavaRDD<String> words = tags.flatMap (tag -> Arrays.asList (tag
                                                                        .toLowerCase ()
                                                                        .split ("\\|")).iterator ())
                                                                        .filter (StringUtils::isNotBlank);
        System.out.println(words.toString ());
        
        // COUNTING
        Map<String, Long> wordCounts = words.countByValue ();
        List<Map.Entry> sorted = wordCounts.entrySet ().stream ()
                                                       .sorted (Map.Entry.comparingByValue ())
                                                       .collect (Collectors.toList ());
        // DISPLAY
        for (Map.Entry<String, Long> entry : sorted) 
        {
            System.out.println (entry.getKey () + " : " + entry.getValue ());
        }
    }
    
    
    
}