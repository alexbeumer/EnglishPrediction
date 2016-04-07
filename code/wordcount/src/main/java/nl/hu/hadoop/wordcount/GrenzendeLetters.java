package nl.hu.hadoop.wordcount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class GrenzendeLetters {

    public static void main(String[] args) throws Exception {
        Job job = new Job();
        job.setJarByClass(GrenzendeLetters.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(GevolgdGetalMapper.class);
        job.setReducerClass(GevolgdGetalReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }
}

class GevolgdGetalMapper extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text valueText, Context context) throws IOException, InterruptedException {
        String value = valueText.toString();
        value = value.replaceAll("[^a-zA-Z ]", "");
        value = value.toLowerCase();
        String[] words = value.split("\\s");

        for (String word : words) {
            char[] letters = word.toCharArray();
            for (int i = 0; i < word.length() - 1; i++) {
                context.write(new Text(Character.toString(letters[i])),  new Text(Character.toString(letters[i + 1])));
            }
        }
    }
}

class GevolgdGetalReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<Character, Integer> optelSomLetterErna = new TreeMap();
        Character[] letters = {'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t',
                'u','v','w','x','y','z'};
        String separatedLetters = "";

        for(Character letter : letters) {
            optelSomLetterErna.put(letter, 0);

            separatedLetters += letter;
            if(letter != 'z') {
                separatedLetters += ",";
            }
        }

        if(key.toString().equals("a")){
            context.write(new Text("Letter,"), new Text(separatedLetters));
        }

        for(Text value : values) {
            char letterErna = value.toString().charAt(0);
            int som = optelSomLetterErna.get(letterErna);
            som++;
            optelSomLetterErna.put(letterErna, som);
        }

        String separatedOptelSomLetterErna = "";

        for(Character character : letters) {
            separatedOptelSomLetterErna += optelSomLetterErna.get(character);
            if(character != 'z') {
                separatedOptelSomLetterErna += ",";
            }
        }

        String separatedKey = key.toString() + ",";
        context.write(new Text(separatedKey), new Text(separatedOptelSomLetterErna));
    }
}
