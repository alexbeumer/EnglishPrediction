package nl.hu.hadoop.wordcount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class EnglishPredictor {

	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(EnglishPredictor.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(EnglishPredictorMapper.class);
		job.setReducerClass(EnglishPredictorReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
	}
}


class EnglishPredictorMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text valueText, Context context) throws IOException, InterruptedException {
		String value = valueText.toString();
		value = value.replaceAll("[^a-zA-Z ]", "");
		value = value.toLowerCase();

		context.write(new Text(value),  new Text());
	}
}

class EnglishPredictorReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Map<Character, Map<Character, Integer>> letterOccurrences = initLetterOccurrences();

		String[] words = key.toString().split("\\s");

		double predictionSum = 0;
		for (String word : words) {
			predictionSum += predict(word, letterOccurrences);
		}

		double sentencePrediction = predictionSum / words.length;
		if(sentencePrediction < 0.5) {
			context.write(new Text(sentencePrediction + ""), key);
		}
	}

	public Map<Character, Map<Character, Integer>> initLetterOccurrences() {
		Character[] letters = {'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t',
				'u','v','w','x','y','z'};
		String csvFilePath = "part-r-00000.csv";
		FileSystem fs;
		BufferedReader br = null;
		String line;
		String cvsSplitBy = ",";

		Map<Character, Map<Character, Integer>> letterOccurrences = new HashMap<>();

		try{
			Path path = new Path(csvFilePath);
			fs = FileSystem.get(new Configuration());

			br = new BufferedReader(new InputStreamReader(fs.open(path)));
			while((line = br.readLine()) != null) {
				String[] values = line.split(cvsSplitBy);

				//Ignore title line
				if(!values[0].equals("Letter")) {
					Character currentCharacter = values[0].charAt(0);
					Map<Character, Integer> innerMap = new HashMap<>();
					for(int i = 1; i < values.length; i++) {
						innerMap.put(letters[i - 1], Integer.parseInt(values[i].trim()));
					}
					letterOccurrences.put(currentCharacter, innerMap);
				}
			}
		}catch (FileNotFoundException e) {
			System.out.println(e);
			e.printStackTrace();
		}catch (IOException e) {
			e.printStackTrace();
		}finally {
			if(br != null) {
				try{
					br.close();
				}catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return letterOccurrences;
	}

	public double predict(String word, Map<Character, Map<Character, Integer>> letterOccurrences) {
		double aantalRedelijkeCombinaties = 0;

		for(int i = 0; i < word.length(); i++) {
			Character previousLetter, currentLetter = word.charAt(i), nextLetter;
			if(i > 0) {
				previousLetter = word.charAt(i - 1);

				if(letterOccurrences.get(previousLetter).get(currentLetter) > 0) {
					aantalRedelijkeCombinaties++;
				}
			}
			if(i < word.length() - 1) {
				nextLetter = word.charAt(i + 1);

				if(letterOccurrences.get(currentLetter).get(nextLetter) > 0) {
					aantalRedelijkeCombinaties++;
				}
			}
		}

		return aantalRedelijkeCombinaties / (word.length() * 2);
	}
}