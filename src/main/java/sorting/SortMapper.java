package sorting;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, Comparator, NullWritable> {
    private final static Comparator outputVal = new Comparator();
    private final static NullWritable outputNull = NullWritable.get();

    /**
     *
     * @param key           row id
     * @param value         text line [title '\t' rank '\t' link1//link2//Link3//...//linkN]
     * @param context       job context
     * @throws IOException
     * @throws InterruptedException
     */
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
