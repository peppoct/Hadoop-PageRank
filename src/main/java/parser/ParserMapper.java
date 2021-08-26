package parser;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utility.Counter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParserMapper extends Mapper<LongWritable, Text, Text, Text>{

    private static final Pattern title_pat = Pattern.compile("<title.*?>(.*?)<\\/title>");
    private static final Pattern text_pat = Pattern.compile("<text.*?>(.*?)<\\/text>");
    private static final Pattern link_pat = Pattern.compile("\\[\\[(.*?)\\]\\]");

    private static final Text outputKey = new Text();
    private static final Text outputVal = new Text();

    /**
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String line = value.toString().replace("\t", " ");
        outputKey.set(getTitle(line));
        String text  = getText(line);
        List<String> links = getOutgoingLinks(text);

        // get the counters to count the number of pages (line)
        context.getCounter(Counter.TOTAL_PAGES).increment(1);

        if (links.size() != 0){

            for (String link : links) {
                outputVal.set(link);
                context.write(outputKey, outputVal);
            }

        } else {
            outputVal.set("");
            context.write(outputKey, outputVal);
        }
    }

    //************************************UTILITY******************************************/

    /**
     *
     * @param str
     * @return return the title of the current page
     */

    private String getTitle(String str){
        Matcher title_match = title_pat.matcher(str);

        //if title exists
        if(title_match.find())
            return title_match.group(1);
        else
            return null;
    }

    /**
     *
     * @param str
     * @return return the text of the current page
     */
    private String getText(String str){
        Matcher text_match = text_pat.matcher(str);

        //if title exists
        if(text_match.find())
            return text_match.group(1);
        else
            return null;
    }

    /**
     *
     * @param str
     * @return return an array of outgoing links
     */
    private List<String> getOutgoingLinks(String str){

        List<String> outgoingLinks = new ArrayList<>();
        Matcher links = link_pat.matcher(str);

        while(links.find()){
            outgoingLinks.add(links.group(1));
        }

        return outgoingLinks;
    }
}

