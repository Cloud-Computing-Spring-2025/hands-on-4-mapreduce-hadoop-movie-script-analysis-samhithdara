package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class CharacterWordMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text characterWord = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        
        // Check if the line contains a dialogue
        if (!line.contains(":")) return;
        
        String[] parts = line.split(":", 2);
        if (parts.length < 2) return;
        
        String dialogue = parts[1].trim();
        
        StringTokenizer st_itr = new StringTokenizer(dialogue.toString());
        while (st_itr.hasMoreTokens()) {
            word.set(st_itr.nextToken());
            context.write(word, one);
        }

    }
}