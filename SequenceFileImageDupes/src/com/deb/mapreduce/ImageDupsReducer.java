package com.deb.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ImageDupsReducer extends Reducer<Text,Text,Text,Text> {

 

                public void reduce(Text key, Iterable<Text> values, Context context)

                                                        throws IOException, InterruptedException {

                        //Key here is the md5 hash while the values are all the image files that

                        // are associated with it. for each md5 value we need to take only

                        // one file (the first)

                        Text imageFilePath = null;

                        for (Text filePath : values) {

                                imageFilePath = filePath;

                                break;//only the first one

                        }

                        // In the result file the key will be again the image file path.

                        context.write(imageFilePath, key);

                }

        }