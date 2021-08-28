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
    private static String OUTPUT_Parsing = "OUTPUT-1";
    private static String OUTPUT_Ranking = "OUTPUT-2";
    private static String FINAL_OUTPUT = "PageRank";
    private static int NUM_REDUCERS;
    private static float ALPHA;
    private static int NUM_ITERATIONS;

    public static void main(String[] args) throws Exception {


        // set configurations
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3){
            System.err.println("Error");
            System.exit(-1);
        }

        deleteFile(conf, OUTPUT_Parsing);
        deleteFile(conf, OUTPUT_Ranking);
        deleteFile(conf, FINAL_OUTPUT);

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
        conf.setFloat("page.alpha", ALPHA);

        System.out.println("[INFO] -> Parsing completed!");

        for (int i = 0; i < NUM_ITERATIONS; i++){
            if(!computePageRankJob(conf, i, NUM_REDUCERS)){
                System.err.println("[ERROR] -> Something wrong in compute phase!");
                System.exit(-1);
            }
        }

        System.out.println("[INFO] -> Computing completed!");

        if (!sortJob(conf, NUM_REDUCERS)){
            System.err.println("[ERROR] -> Something wrong in sort phase!");
            System.exit(-1);
        }

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
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_Parsing));

        boolean check = job.waitForCompletion(true);
        if (check)
            return job.getCounters().findCounter(Counter.TOTAL_PAGES).getValue();
        else
            return -1;
    }

    private static boolean computePageRankJob(Configuration conf, int iteration, int numReducers) throws Exception{
        Job job = Job.getInstance(conf, "compute");
        job.setJarByClass(Driver.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);

        // set number of reducer tasks to be used
        job.setNumReduceTasks(numReducers);

        // CHECK IF STEP 1
        if (iteration == 0) {
            FileInputFormat.setInputPaths(job,
                    new Path(OUTPUT_Parsing + "/part-r-00000")/*,
                    new Path(OUTPUT_Parsing + "/part-r-00002")*/);
            FileOutputFormat.setOutputPath(job, new Path(OUTPUT_Ranking + "/iter" + (iteration+1)));
        } else {
            FileInputFormat.setInputPaths(job,
                    new Path(OUTPUT_Ranking + "/iter" + (iteration) + "/part-r-00000")/*,
                    new Path(OUTPUT_Ranking + "/iter" + (iteration) + "/part-r-00001"),
                    new Path(OUTPUT_Ranking + "/iter" + (iteration) + "/part-r-00002")*/);
            FileOutputFormat.setOutputPath(job, new Path(OUTPUT_Ranking + "/iter" + (iteration+1)));
        }

        /*
        FileInputFormat.setInputPaths(job, inputs(OUTPUT_Parsing));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_Ranking));
         */

        boolean result = job.waitForCompletion(true);
        /*
        if (iteration == 0)
            deleteFile(conf, OUTPUT_Parsing);
        if (iteration > 0)
            deleteFile(conf, OUTPUT_Ranking + "/iter" + iteration);

         */

        return result;
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

        FileInputFormat.setInputPaths(job,  new Path(OUTPUT_Ranking + "/iter" + NUM_ITERATIONS + "/part-r-00000")/*,
                new Path(OUTPUT_Ranking + "/iter" + (NUM_ITERATIONS) + "/part-r-00001"),
                new Path(OUTPUT_Ranking + "/iter" + (NUM_ITERATIONS) + "/part-r-00002")*/);
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
