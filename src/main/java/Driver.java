import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import parser.ParserMapper;
import parser.ParserReducer;
import ranking.PageRankMapper;
import ranking.PageRankReducer;
import sorting.Comparator;
import sorting.SortMapper;
import sorting.SortReducer;
import utility.Counter;

public class Driver {
    private static String INPUT_PATH;
    private static String OUTPUT1_PATH = "OUTPUT-1";
    private static String OUTPUT2_PATH = "OUTPUT-2";
    private static String FINAL_OUTPUT = "PageRank";
    private static int NUM_REDUCERS;
    private static float ALPHA;
    private static float NUM_ITERATIONS;

    public static void main(String[] args) throws Exception {
        // set configurations
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3){
            System.err.println("Error");
            System.exit(-1);
        }

        System.out.println("args[0]: <input>\t"+otherArgs[0]);
        System.out.println("args[1]: <input>\t"+otherArgs[1]);
        System.out.println("args[1]: <input>\t"+otherArgs[2]);

        INPUT_PATH = otherArgs[0];
        ALPHA = Float.parseFloat(otherArgs[1]);
        NUM_ITERATIONS = Integer.parseInt(otherArgs[2]);
        NUM_REDUCERS = 1;

        //First phase
        //deleteFile(conf, 1);
        long numpages = parserJob(conf, NUM_REDUCERS);
        if (numpages < 0){
            System.err.println("[ERROR] -> Something wrong in parser phase!");
            System.exit(-1);
        }
        //set the pageCount on the configuration
        conf.setLong("page.num", numpages);

        System.out.println("[INFO] -> Parsing completed!");


        conf.setFloat("page.alpha", ALPHA);

        for (int i = 0; i < NUM_ITERATIONS; i++){
            if(!computePageRankJob(conf, NUM_REDUCERS)){
                System.err.println("[ERROR] -> Something wrong in compute phase!");
                System.exit(-1);
            }
            deleteFile(conf, OUTPUT1_PATH);
            String tmp = OUTPUT1_PATH;
            OUTPUT1_PATH = OUTPUT2_PATH;
            OUTPUT2_PATH = tmp;
        }

        System.out.println("[INFO] -> Computing completed!");

        deleteFile(conf, FINAL_OUTPUT);

        if (!sortJob(conf, NUM_REDUCERS)){
            System.err.println("[ERROR] -> Something wrong in sort phase!");
            System.exit(-1);
        }

        deleteFile(conf, OUTPUT2_PATH);
        deleteFile(conf, OUTPUT1_PATH);
        System.out.println("[INFO] -> Sort completed!");
        //Eliminare file di output intermedi

    }

    private static long parserJob(Configuration conf, int numReducers) throws Exception{
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
        if (check)
            return job.getCounters().findCounter(Counter.TOTAL_PAGES).getValue();
        else
            return -1;
    }

    private static boolean computePageRankJob(Configuration conf, int numReducers) throws Exception{
        Job job = Job.getInstance(conf, "compute");
        job.setJarByClass(Driver.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);

        // set number of reducer tasks to be used
        job.setNumReduceTasks(numReducers);

        FileInputFormat.addInputPath(job, new Path(OUTPUT1_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT2_PATH));

        return job.waitForCompletion(true);
    }

    private static boolean sortJob(Configuration conf, int numReducers) throws Exception{
        Job job = Job.getInstance(conf, "sort");
        job.setJarByClass(Driver.class);

        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setSortComparatorClass(Comparator.class);

        // set number of reducer tasks to be used
        job.setNumReduceTasks(numReducers);

        FileInputFormat.addInputPath(job, new Path(OUTPUT1_PATH));
        FileOutputFormat.setOutputPath(job, new Path(FINAL_OUTPUT));

        return job.waitForCompletion(true);
    }



    //*********************************UTILITY**************************************/
    //  removes old outputs that has to be overwritten by hadoop jobs
    private static void deleteFile(Configuration conf, String path){

        Path filePath = new Path(path);
        try {
            FileSystem fs = filePath.getFileSystem(conf);
            if (fs.exists(filePath)) {
                fs.delete(filePath, true);
            }
        }catch(Exception e){
            e.printStackTrace();
        }

    }
}
