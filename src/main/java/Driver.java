import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;

public class Driver {

    public static void main(String[] args) throws Exception {
        // set configurations
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2){
            System.err.println("Error");
            System.exit(-1);
        }

        System.out.println("args[0]: <input>"+otherArgs[0]);
        System.out.println("args[1]: <input>"+otherArgs[1]);

        // instantiate job
        Job job = new Job(conf, "Count");
        //job.setJarByClass(Count.class);
        // set mapper/combiner/reducer
        /*job.setMapperClass(CountMapper.class);
        job.setCombinerClass(CountReducer.class);
        job.setReducerClass(CountReducer.class);
        // define mapper's output key-value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);*/

        // define I/O //passiamo il file
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // define input/output format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //job.setOutputFormatClass(SequenceFileOutputFormat.class);

        System.out.println(job.waitForCompletion(true));
    }

    public boolean parser(int numReducers) throws Exception{
        Configuration conf = new Configuration();
        Job job = new Job(conf, "parser");
        job.setJarByClass(Parse.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //set the pageCount on the configuration
        job.getConfiguration().setInt("page.count", pageCount);

        job.setMapperClass(ParseMapper.class);
        job.setReducerClass(ParseReducer.class);

        // set number of reducer tasks to be used
        job.setNumReduceTasks(numReducers);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job.waitForCompletion(true);
    }
}
