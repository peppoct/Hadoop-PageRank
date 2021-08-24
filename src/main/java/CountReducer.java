import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable N = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        int numpages = 0;
        for (IntWritable val : values){
            numpages += val.get();
        }

        N.set(numpages);
        context.write(key, N);

    }
}
