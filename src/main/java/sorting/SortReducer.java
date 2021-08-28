package sorting;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.checkerframework.checker.units.qual.C;

import java.io.IOException;

public class SortReducer extends Reducer<Comparator, NullWritable, Text, Text> {

    private final static Text text = new Text();
    private final static Text outputVal = new Text();
    private final static Text outputKey = new Text();

    public void reduce(Comparator key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            outputVal.set(Double.toString(key.getRank()));
            outputKey.set(key.getTitle());
            context.write(outputKey, outputVal);
    }
}


