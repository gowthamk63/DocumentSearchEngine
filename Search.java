//Gowtham Kommineni
//gkommine@uncc.edu

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;


public class Search extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( Search.class);

   public static void main( String[] args) throws  Exception {
	   
      int res  = ToolRunner .run( new Search(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
	  Configuration config = getConf();
	  FileSystem FS = FileSystem.get(config);
	   //passing query from arguments with conf obj
	  String query = args[2];
	  config.set("query", query);   
	   
      Job job  = Job .getInstance(getConf(), " search ");
      job.setJarByClass( this .getClass());
      
      FileInputFormat.addInputPaths(job, args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[1]));  
      
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( DoubleWritable .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,DoubleWritable> {
	   
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
    	  
         String line  = lineText.toString();
         
         //Reading query from conf obj
         String[] query = context.getConfiguration().get("query").toLowerCase().split(" ");
         
	 //Getting TFIDF  	 
         String wordName = line.split("#####")[0]; 
         String term = line.split("#####")[1];
         String fileName = term.split("\\t")[0];
         Double tfidf = Double.parseDouble(term.split("\\t")[1]);
         
	 //passing filename along with tfidf value for the given query
         for(String word : query){
        	//Searching for query 
        	 if(word.equals(wordName)){
        		 context.write(new Text(fileName), new DoubleWritable(tfidf));
        	 }
         }
      }
   }

   public static class Reduce extends Reducer<Text ,  DoubleWritable ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text word,  Iterable<DoubleWritable> tf_idf,  Context context)
         throws IOException,  InterruptedException {
    	  
    	  double score = 0.0;
  	  
    	  //Adding scores for a file
    	  for(DoubleWritable value : tf_idf){
    		score += value.get();
    	  }
    	  
    	  context.write(word, new DoubleWritable(score));
      }
   }
}
