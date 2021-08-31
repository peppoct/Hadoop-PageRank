package ranking;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {
    private static final Text outputVal = new Text();
    private static long numpages;
    private static float alpha;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        alpha = context.getConfiguration().getFloat("page.alpha", 0.85f);
        numpages = context.getConfiguration().getLong("page.num", 0);
        if (numpages == 0)
            System.exit(-1);
    }

    //title     rank
    //title     1   outlinks

    /**
     *
     * @param key           title
     * @param values        all the outlinks of title with their rank [rank '\t' link1//link2//...//linkN]
     * @param context       job context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String outgoinglinks = "";
        double pageRank = 0.0;

        for (Text value : values){
            String[] list = value.toString().split("\t");
            if(list.length > 1) {
                outgoinglinks = list[1];
                continue;
            }
             pageRank += Double.parseDouble(value.toString());
        }

        if (!outgoinglinks.equals("")) {
            double val = ((1 - alpha) * pageRank + (alpha / numpages));
            outputVal.set(val + "\t" + outgoinglinks);
            context.write(key, outputVal);
        }
    }
}