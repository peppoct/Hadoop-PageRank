import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        String title = split[0];
        double rank = Double.parseDouble(split[1]);

        if (split.length < 3)
            return;

        String[] outgoingLinks = split[2].split(",");

        for (String outgoingLink : outgoingLinks){
            double outgoingLinkRank = rank / outgoingLinks.length;
            context.write(new Text((outgoingLink)), new Text(Double.toString(outgoingLinkRank)));
        }
    }
}
