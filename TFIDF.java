//Gowtham Kommineni
//gkommine@uncc.edu

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class TFIDF extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( TFIDF.class);

   public static void main( String[] args) throws  Exception {
	//passing intermediate output path to arguments to TermFrequency and TFIDF
	  String[] TF_args={args[0],"intermediate"};
	  int res1 = ToolRunner .run( new TermFrequency(),TF_args);
      int res  = ToolRunner .run( new TFIDF(),args);

      System.exit(res);
      System .exit(res1);
   }

   public int run( String[] args) throws  Exception {
	  Configuration conf=getConf();
	  FileSystem file_system=FileSystem.get(conf);
	  
	  //Passing the num of files with conf obj
	  final int num_files= file_system.listStatus(new Path(args[0])).length;
	  conf.setInt("num_files", num_files);
	  
      Job job  = Job .getInstance(getConf(), " tfidf ");
      job.setJarByClass( this .getClass());
      
      FileInputFormat.addInputPaths(job,  "intermediate");
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( Text .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
      private final static IntWritable one  = new IntWritable( 1);
      
      private Text word  = new Text();

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString();
         Text currentWord  = new Text();
         Text currentCount = new Text();
         
         //Extracting words and filenames
	 String term = line.split("#####")[1];
         String wordName = line.split("#####")[0];
         String fileName = term.split("\\t")[0];
         String value = fileName + "=" + term.split("\\t")[1];
         currentWord  = new Text(wordName);
         currentCount  = new Text(value);
         //Writing word and counts to reducer
         context.write(currentWord,currentCount);
         
         }
      }

   public static class Reduce extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text word,  Iterable<Text > value,  Context context)
         throws IOException,  InterruptedException {
         Configuration conf=context.getConfiguration();
         //Reading num of files from conf obg
         int numFiles=conf.getInt("num_files",0);
         int numDocs=0;
         
         //Using Array list to store the file names and frequencies
         List<String> fnames= new ArrayList<String>();
         List<Double> freq=new ArrayList<Double>();
         
         
         for ( Text val  : value) {
            numDocs  += 1;
            fnames.add(val.toString().split("=")[0]);
            freq.add(Double.parseDouble(val.toString().split("=")[1]));
         }
         double tf_idf;
         
         //Calculating IDF values
         double idf=Math.log10(1+numFiles/numDocs);
         
         //Computing TFIDF values 
         for(int i = 0;i<fnames.size();i++){
   		  tf_idf = idf * freq.get(i);
   		  context.write(new Text(word.toString()+"#####"+fnames.get(i)),new DoubleWritable(tf_idf));
   	  }
      }
   }
}

