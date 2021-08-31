package sorting;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class SortReducer extends Reducer<Comparator, NullWritable, Text, Text> {
    private final static Text outputVal = new Text();
    private final static Text outputKey = new Text();

    /**
     *
     * @param key           Comparator istance
     * @param values        nothing
     * @param context       job context
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Comparator key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            outputVal.set(Double.toString(key.getRank()));
            outputKey.set(key.getTitle());
            context.write(outputKey, outputVal);
    }
}


