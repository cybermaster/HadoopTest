import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountJob {
  private final static LongWritable one = new LongWritable(1); 
  private Path inputFile = null;
  private Path outputFile = null;
  private Job job = null;
  static Logger log = Logger.getLogger(WordCountJob.class.getName());

  public WordCountJob(String inFile, String outFile) {
    inputFile = new Path(inFile);
    outputFile = new Path(outFile);
    generate();
  }
  
  public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private Text word = new Text();
    @Override
    protected void setup(Context context) {
      System.out.println("Mapper.setup called");
    }
   
    @Override
    protected void map(LongWritable key, Text value, Context context) {
      try {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
          word.set(itr.nextToken());
          context.write(word, one);
        }
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    
    @Override 
    protected void cleanup(Context context) {
      System.out.println("Mapper.cleanup called");
    }
  }
  
  public static class WordCountReducer extends Reducer<Text, LongWritable, Text, NullWritable> {
    private Text count = new Text();
    
    @Override
    protected void setup(Context context) {
      System.out.println("Reducer.setup called");
    }
    
    
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) {
      try {
        long sum = 0l;
        for (LongWritable val : values) {
          sum += val.get();
        }
        count.set(key + "," + sum);
        context.write(count, null);
        
        //System.out.println(count);
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    
    @Override 
    protected void cleanup(Context context) {
      System.out.println("Reducer.cleanup called");
    }
  }
  
  private void generate() {
    try {
      job = new Job();
      
      job.setMapperClass(WordCountMapper.class);
      job.setReducerClass(WordCountReducer.class);

      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(LongWritable.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(LongWritable.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.setInputPaths(job, inputFile);
      FileOutputFormat.setOutputPath(job, outputFile);
      
      job.setNumReduceTasks(1);
      job.setJarByClass(WordCountJob.class);
      System.out.println(job.getPartitionerClass());
      
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }
  
  public Job getJob() { return job; } 
  
  public void prepareJob() {
    try {
      FileSystem fs = outputFile.getFileSystem(job.getConfiguration());
      fs.delete(outputFile, true);
    } catch (IOException e) {
      e.printStackTrace();
    }
  
  }
  
  public static void main(String[] args) {
      log.debug("Hello this is an debug message");
      log.info("Hello this is an info message");
    if (args.length < 1) {
      System.out.println("Too few parameters");
      System.exit(1);
    }
    String inputFile = args[0];
    String outputFile = inputFile + "_counts_hadoop";
    try {
      WordCountJob wcJob = new WordCountJob(inputFile, outputFile);
      wcJob.prepareJob();
      wcJob.getJob().submit();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }
}
