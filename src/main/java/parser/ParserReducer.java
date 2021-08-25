package parser;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class ParserReducer extends Reducer<Text, Text, Text, Text> {

    private final Text outValue = new Text();

    /**
     * Reducer used to store the results calculated with the mappers
     * @param key title of the page
     * @param values outgoing links of this page
     * @param context job cotext
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        String outgoinglinks = "";

        for (Text link : values)
            outgoinglinks += link.toString() + ",";

        String list = 1 + "\t" + outgoinglinks;
        outValue.set(list);
        context.write(key, outValue);
    }
}
