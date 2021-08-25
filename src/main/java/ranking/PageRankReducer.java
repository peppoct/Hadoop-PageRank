package ranking;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {

    private static float alpha;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        alpha = Float.parseFloat(context.getConfiguration().get("page.alpha"));
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String outgoingLinks = "";
        double pageRank = 0.0;
        String str;

        for (Text text : values){
            str = text.toString();
            /*
            if (){

            } else {
                pageRank += Double.parseDouble((str));
            }

             */
        }

        pageRank = (1 - alpha) + alpha * pageRank;
        context.write(new Text(), new Text(Double.toString(pageRank) + outgoingLinks));

    }
}
