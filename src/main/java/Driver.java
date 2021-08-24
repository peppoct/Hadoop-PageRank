import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import parser.ParserMapper;
import parser.ParserReducer;
import utility.Counter;

public class Driver {
    private static String INPUT_PATH;
    private final static String OUTPUT1_PATH = "OUTPUT-1";
    private static int NUM_REDUCERS;
    private static double ALFA;

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

        INPUT_PATH = otherArgs[0];
        ALFA = Double.parseDouble(otherArgs[1]);
        NUM_REDUCERS = 1;

        if (!parserJob(conf, NUM_REDUCERS)){
            System.err.println("[ERROR] -> Something wrong in parser phase!");
            System.exit(-1);
        }

        System.out.println("[INFO] -> Parsing completed!");


        /*
        // instantiate job
        Job job = new Job(conf, "Count");
        //job.setJarByClass(Count.class);
        // set mapper/combiner/reducer
        job.setMapperClass(CountMapper.class);
        job.setCombinerClass(CountReducer.class);
        job.setReducerClass(CountReducer.class);
        // define mapper's output key-value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // define I/O //passiamo il file
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // define input/output format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //job.setOutputFormatClass(SequenceFileOutputFormat.class);

        System.out.println(job.waitForCompletion(true));
         */
    }

    private static boolean parserJob(Configuration conf, int numReducers) throws Exception{
        Job job = Job.getInstance(conf, "parser");
        job.setJarByClass(Driver.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(ParserMapper.class);
        job.setReducerClass(ParserReducer.class);

        // set number of reducer tasks to be used
        job.setNumReduceTasks(numReducers);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT1_PATH));

        boolean check = job.waitForCompletion(true);

        //set the pageCount on the configuration
        job.getConfiguration().setLong("page.num", job.getCounters().findCounter(Counter.TOTAL_PAGES).getValue());
        ;
        return check;
    }
}
