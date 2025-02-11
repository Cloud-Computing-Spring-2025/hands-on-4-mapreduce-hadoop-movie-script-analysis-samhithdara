package com.movie.script.analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringJoiner;

public class UniqueWordsReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashSet<String> uniqueWords = new HashSet<>();

        // Collect unique words spoken by the character
        for (Text value : values) {
            uniqueWords.add(value.toString());
        }

        // Join the unique words into a formatted list
        StringJoiner wordList = new StringJoiner(", ", "[", "]");
        for (String word : uniqueWords) {
            wordList.add(word);
        }

        context.write(key, new Text(wordList.toString()));
    }
}
