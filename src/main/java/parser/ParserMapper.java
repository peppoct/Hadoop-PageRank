package parser;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParserMapper extends Mapper<LongWritable, Text, Text, Text>{
    private static final Pattern title_pat = Pattern.compile("<title.*?>(.*?)<\\/title>");
    private static final Pattern text_pat = Pattern.compile("<text.*?>(.*?)<\\/text>");
    private static final Pattern link_pat = Pattern.compile("\\[\\[(.*?)\\]\\]");

    private static final Text outputKey = new Text();
    private static final Text outputVal = new Text();

    /**
     * Mapper used to parse a text line and retrive the page title and all its own outlinks
     * @param key       row id
     * @param value     text line
     * @param context   job context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String line = value.toString().replace("\t", " ");
        String title = getTitle(line);
        String text  = getText(line);
        ArrayList<String> links = getOutgoingLinks(text);

        outputKey.set(title);
        String outgoingLinks = "";

        for (String link : links)
            outgoingLinks += link + "//";

        String list = "1.0\t" + outgoingLinks;
        outputVal.set(list);
        context.write(outputKey, outputVal);
    }

    //************************************UTILITY******************************************/

    /**
     *
     * @param str text line
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
     * @param str text line
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
     * @param str text line
     * @return return an array of outgoing links
     */
    private ArrayList<String> getOutgoingLinks(String str){
        ArrayList<String> outgoingLinks = new ArrayList<>();
        Matcher links = link_pat.matcher(str);

        while(links.find())
            outgoingLinks.add(links.group(1));

        return outgoingLinks;
    }
}

