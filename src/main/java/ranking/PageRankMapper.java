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
        numpages = Long.parseLong(context.getConfiguration().get("page.num"));
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");

        if (split.length < 3)
            return;

        String title = split[0];
        double rank = Double.parseDouble(split[1]);
        if(rank == 1)
            rank = 1/(double)numpages;

        outputKey.set(title);
        outputVal.set(rank + "\t" + split[2]);
        context.write(outputKey, outputVal);

        String[] outgoingLinks = split[2].split(",");

        for (String outgoingLink : outgoingLinks){
            double outgoingLinkRank = rank / outgoingLinks.length;
            outputKey.set(outgoingLink);
            outputVal.set(Double.toString(outgoingLinkRank));
            context.write(outputKey, outputVal);
        }
    }
}
