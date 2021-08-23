import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParserMapper extends Mapper<LongWritable, Text, Text, Text>{

    private static final Pattern title_pat = Pattern.compile("<title>(.*)</title>");
    private static final Pattern text_pat = Pattern.compile("<text(.*?)</text>");
    private static final Pattern link_pat = Pattern.compile("\\[\\[(.*?)\\]\\]");


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String line = value.toString();
        String title = getTitle(line);
        String text  = getText(line);
        Text[] link = getOutgoingLinks(line);
        if (link.length != 0){
            for (int i=0; i<link.length; i++)
                context.write(new Text(title), link[i]);
        } else {
            context.write(new Text(title), new Text(""));
        }
    }

    private String getTitle(String str){
        Matcher title_match = title_pat.matcher(str);

        //if title exists
        if(title_match.find())
            return title_match.group(1);
        else
            return null;
    }

    private String getText(String str){
        Matcher text_match = text_pat.matcher(str);

        //if title exists
        if(text_match.find())
            return text_match.group(1);
        else
            return null;
    }

    private Text[] getOutgoingLinks(String str){

        List<Text> outgoingLinks = new ArrayList<>();
        Matcher links = link_pat.matcher(str);

        while(links.find()){
            outgoingLinks.add(new Text(links.group(1)));
        }


        Text[] arrayLinks = new Text[outgoingLinks.size()];
        outgoingLinks.toArray(arrayLinks);
        return arrayLinks;
    }
}

