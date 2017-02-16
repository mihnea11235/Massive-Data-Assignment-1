package mdp.invidx;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Qd extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new Qd(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "Qd");
      job.setJarByClass(Qd.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);
  	  job.setNumReduceTasks(1);
      
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      
      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", "=>");

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
      return 0;
   }
   
   public static class Map extends Mapper<LongWritable, Text, Text, Text> {
     
      private Text word = new Text();
      private Text flname = new Text();

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	  
    	  HashSet<String> StWds = new HashSet<String>();
    	  BufferedReader Rdr = new BufferedReader(new FileReader(new File("/home/cloudera/workspace/InvertedIndex/StopWordsText.txt")));
    	  
    	  String ptrn;
    	  
    	  while ((ptrn = Rdr.readLine()) != null){
    		  StWds.add(ptrn.toLowerCase());
    	  }
    	  
    	  String FileNameStr = ((FileSplit) context.getInputSplit()).getPath().getName();
    	  flname = new Text(FileNameStr);
    	  
         for (String token: value.toString().split("\\s+")) {
        	 if (!StWds.contains(token.toLowerCase())){
        		 word.set(token.toLowerCase());
        	 }
         }
         context.write(word, flname);
      }
      
   }

   public static class Reduce extends Reducer<Text, Text, Text, Text> {
      @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
         
    	  ArrayList<String> word_collect = new ArrayList<String>();
    	  
    	  for (Text val : values){
    		  word_collect.add(val.toString());
    	  }
    	  
    	  HashSet<String> Set = new HashSet<String>(word_collect);
    	  StringBuilder bld = new StringBuilder();
    	  
    	  String pref = "";
    	  
    	  for (String val:Set){
    		  bld.append(pref);
    		  pref = ", ";
    		  bld.append(val+"#"+Collections.frequency(word_collect, val));
    	  }
    	  
    	  context.write(key, new Text(bld.toString()));
      }
   }
}
