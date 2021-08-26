package ranking;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {
    private static final Text outputKey = new Text();
    private static final Text outputVal = new Text();
    private static long numpages;
    private static float alpha;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        alpha = context.getConfiguration().getFloat("page.alpha", 0.85f);
        numpages = context.getConfiguration().getLong("page.num", 0);
    }

    //title     rank
    //title     1   outlinks
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String outgoinglinks = "";
        double pageRank = 0.0;

        outputKey.set(key);

        for (Text value : values){
            String[] list = value.toString().split("\t");
            if(Double.parseDouble(list[0]) == 1.0) {
                outgoinglinks = list[1];
                continue;
            }
             pageRank += Double.parseDouble(value.toString());
        }

        outputVal.set(((1 - alpha) * pageRank + (alpha/numpages)) + "\t" + outgoinglinks);
        context.write(outputKey, outputVal);
    }
}