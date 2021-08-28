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
        String title = getTitle(line);
        String text  = getText(line);
        ArrayList<String> links = getOutgoingLinks(text);

        // get the counters to count the number of pages (line)
        context.getCounter(Counter.TOTAL_PAGES).increment(1);

        if (title.equals("") || title == null)
            return;

        outputKey.set(title);

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
        /*
        Matcher title_match = title_pat.matcher(str);

        //if title exists
        if(title_match.find())
            return title_match.group(1);
        else
            return null;

         */

        String initialString = "<title>";
        // document.indexOf() returns the index of the first character
        return str.substring(
                str.indexOf(initialString) + initialString.length(), // I need to sum the length of the string
                str.indexOf("</title>"));
    }

    /**
     *
     * @param str
     * @return return the text of the current page
     */
    private String getText(String str){
        /*
        Matcher text_match = text_pat.matcher(str);

        //if title exists
        if(text_match.find())
            return text_match.group(1);
        else
            return null;

         */

        String startOfTextSection = "<text";
        int start = str.indexOf(startOfTextSection);
        // from the end of '<text' go to '>', discarding possible attribute
        start = str.indexOf(">", start + startOfTextSection.length());

        return str.substring(
                start + 1, // start from the character following '>'
                str.indexOf("</text>"));
    }

    /**
     *
     * @param str
     * @return return an array of outgoing links
     */
    private ArrayList<String> getOutgoingLinks(String str){

        ArrayList<String> outgoingLinks = new ArrayList<>();
        /*
        Matcher links = link_pat.matcher(str);

        while(links.find()){
            outgoingLinks.add(links.group(1));
        }

        return outgoingLinks;

         */

        int i=0;
        while (true)
        {
            String initialString = "[[";
            int start = str.indexOf(initialString, i); // Starting from i
            if (start == -1) break;
            int end = str.indexOf("]]", start); // Starting from start
            outgoingLinks.add(
                    str.substring(start + initialString.length(), end)
            );
            i = end + 1; // Advance i for the next iteration
        }

        return outgoingLinks;
    }
}

