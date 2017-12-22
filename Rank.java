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


public class Rank extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( Rank.class);

   public static void main( String[] args) throws  Exception {
	   
      int res  = ToolRunner .run( new Rank(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
	   
      Job job  = Job .getInstance(getConf(), " rank ");
      job.setJarByClass( this .getClass());
      
      FileInputFormat.addInputPaths(job, args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[1]));  
      
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( DoubleWritable .class);
      job.setOutputValueClass( Text .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  DoubleWritable, Text> {
	   
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
    	  
         String line  = lineText.toString();
         
         String fName = line.split("\\t")[0];
         Double val = Double.parseDouble(line.split("\\t")[1]);
        
        //negating the value so that reducer sorts in descending order
        context.write(new DoubleWritable(-val), new Text(fName));
      }
   }

   public static class Reduce extends Reducer<DoubleWritable , Text,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( DoubleWritable sValue,  Iterable<Text> fNames,  Context context)
         throws IOException,  InterruptedException {
    	  
    	  Double val = sValue.get();
    	  
    	  for(Text name: fNames){
    		  //Printing the negative value to remove the minus sign added at mapper
        	  context.write(new Text(fName), new DoubleWritable(-val));
    	  }
      }
   }
}
