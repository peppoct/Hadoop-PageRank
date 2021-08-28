package parser;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ParseCombiner extends Reducer<Text, Text, Text, Text> {

    //private static String value;
    //private static boolean emptyFound;
    private final Text outValue = new Text();

  /*  @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String outgoingLinks = "";

        for (Text link : values)
            outgoingLinks += link.toString() + "//";

        String list = "1.0\t" + outgoingLinks;
        outValue.set(list);
        context.write(key, outValue);
    }*/
}
