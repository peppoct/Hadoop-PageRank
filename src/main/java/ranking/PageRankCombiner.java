package ranking;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class PageRankCombiner extends Reducer<Text, Text, Text, Text> {
    private static final Text outputVal = new Text();

    /**
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double pageRank = 0.0;

        for (Text value : values) {
            String[] list = value.toString().split("\t");
            if (list.length > 1) {
                context.write(key, value);
                continue;
            }
            pageRank += Double.parseDouble(value.toString());
        }

        outputVal.set(Double.toString(pageRank));
        context.write(key, outputVal);
    }
}