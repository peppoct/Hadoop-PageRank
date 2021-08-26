package sorting;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

    private final static DoubleWritable outputKey = new DoubleWritable();
    private final static Text outputVal = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split("\t");
        String title = split[0];
        double rank = Double.parseDouble(split[1]);
        outputKey.set(rank);
        outputVal.set(title);
        context.write(outputKey, outputVal);

    }
}
