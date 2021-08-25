import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import parser.ParserMapper;
import parser.ParserReducer;
import ranking.PageRankMapper;
import ranking.PageRankReducer;
import utility.Counter;

public class Driver {
    private static String INPUT_PATH;
    private final static String OUTPUT1_PATH = "OUTPUT-1";
    private final static String OUTPUT2_PATH = "OUTPUT-2";
    private static int NUM_REDUCERS;
    private static float ALPHA;

    public static void main(String[] args) throws Exception {
        // set configurations
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2){
            System.err.println("Error");
            System.exit(-1);
        }

        System.out.println("args[0]: <input>\t"+otherArgs[0]);
        System.out.println("args[1]: <input>\t"+otherArgs[1]);
        System.out.println("args[1]: <input>\tSET");

        INPUT_PATH = otherArgs[0];
        ALPHA = Float.parseFloat(otherArgs[1]);
        NUM_REDUCERS = 1;

        //First phase
        deleteFile(conf, 1);
        if (!parserJob(conf, NUM_REDUCERS)){
            System.err.println("[ERROR] -> Something wrong in parser phase!");
            System.exit(-1);
        }

        System.out.println("[INFO] -> Parsing completed!");

        conf.setDouble("page.alfa", ALPHA);
        /*
        if(!computePageRankJob(conf, NUM_REDUCERS)){
            System.err.println("[ERROR] -> Something wrong in compute phase!");
            System.exit(-1);
        }
         */

        System.out.println("[INFO] -> Computing completed!");

        //Eliminare file di output intemedi
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

        return check;
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



    //*********************************UTILITY**************************************/
    //  removes old outputs that has to be overwritten by hadoop jobs
    private static void deleteFile(Configuration conf, int index){

        Path filePath = new Path("OUTPUT-"+index);
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
