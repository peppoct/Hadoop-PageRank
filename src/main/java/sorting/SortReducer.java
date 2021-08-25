package sorting;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<DoubleWritable, Text, Text, Text> {

    private final static Text text = new Text();

    public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text title: values) {
            text.set(String.valueOf(key));
            context.write(title, text);
        }
    }

}
