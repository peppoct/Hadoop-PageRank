package sorting;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, Comparator, NullWritable> {

    private final static DoubleWritable outputKey = new DoubleWritable();
    private final static Comparator outputVal = new Comparator();
    private final static NullWritable outputNull = NullWritable.get();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split("\t");
        String title = split[0];
        double rank = Double.parseDouble(split[1]);

        outputVal.setTitle(title);
        outputVal.setRank(rank);
        context.write(outputVal, outputNull);

    }
}
