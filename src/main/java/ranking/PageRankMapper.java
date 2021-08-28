package ranking;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static long numpages;
    private static final Text outputKey = new Text();
    private static final Text outputVal = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        numpages = context.getConfiguration().getLong("page.num", 0);
        if (numpages < 1)
            System.exit(-1);
    }

    // title    1   outgoinglinks
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().trim().split("\t");

        String title = split[0];
        outputKey.set(title);

        double rank = Double.parseDouble(split[1]);
        if(rank == 1.0)
            rank = (double) 1/numpages;

        //page without outgoing links
        if (split.length < 3){
            outputVal.set(Double.toString(rank));
            context.write(outputKey, outputVal);
            return;
        }

        //emit the structure
        outputVal.set(1.0 + "\t" + split[2]);
        context.write(outputKey, outputVal);

        String[] outgoingLinks = split[2].split("//");

        for (String outgoingLink : outgoingLinks){
            double outgoingLinkRank = rank/outgoingLinks.length;
            outputKey.set(outgoingLink);
            outputVal.set(Double.toString(outgoingLinkRank));
            context.write(outputKey, outputVal);
        }
    }
}
