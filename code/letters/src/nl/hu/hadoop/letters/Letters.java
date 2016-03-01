package nl.hu.hadoop.letters;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Letters {

	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(Letters.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(LettersMapper.class);
		job.setReducerClass(LettersReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
	}
}

class LettersMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable Key, Text valueText, Context context) throws IOException, InterruptedException {
		String value = valueText.toString().toLowerCase().replaceAll("[^a-z]", "");
		String[] words = value.split("\\s");
		for (String word : words) {
            char[] letters = word.toCharArray();
			for(int i = 0; i < word.length() - 1; i++) {
                context.write(new Text(Character.toString(letters[i])), new Text(Character.toString(letters[i + 1])));
            }
		}
	}
}

class LettersReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<Character, Integer> lettersErna = new HashMap<Character, Integer>();
        for(Text value : values) {
            Character val = value.toString().charAt(0);
            if(lettersErna.containsKey(val)){
                int valuevankey = lettersErna.get(val);
                valuevankey++;
                lettersErna.put(val, valuevankey);
            }
            else
            {
                lettersErna.put(val, 1);
            }
        }
        context.write(key, new Text(lettersErna.toString()));
	}
}