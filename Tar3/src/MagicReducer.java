import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class MagicReducer extends Reducer<Text, Text, NullWritable, Text> {

    private  Map<String,String> tokenMap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("Start reduce setup");
        tokenMap = new HashMap<String, String>();

        Configuration conf = context.getConfiguration();
        Path path = new Path(conf.get("filter_path"));//Location of file in HDFS

        System.out.println("filter_file: " + path.toString());

        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader bReader = new BufferedReader(new InputStreamReader(fs.open(path)));

        System.out.println("start building dictionary");
        try {
            // Read the filter file
            for (String s = null; (s = bReader.readLine()) != null; ) {
                tokenMap.put(String.valueOf(s.length()) + s.charAt(s.length() - 1), s + ": ");
                System.out.println("s: " + s);
            }
        }
        catch(IOException e) {
            System.out.println("ERROR!: "+ e.getStackTrace());
        }
        System.out.println("end building dictionary");
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // filter the result by the filter file
        if (tokenMap.get(key.toString()) != null) {
            System.out.println(key.toString());
            for (Text val : values) {
                tokenMap.put(key.toString(), tokenMap.get(key.toString()) + val.toString() + ", ");
                System.out.println("Key: " + key.toString() + ", Value: " + tokenMap.get(key.toString()));
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Text outputKeyText = new Text();
        Text outputValueText = new Text();

        for (String s : tokenMap.keySet()) {
            outputKeyText.set(s);
            outputValueText.set(tokenMap.get(s).substring(0, tokenMap.get(s).length() - 2));

            context.write(NullWritable.get(), outputValueText);
        }
    }
}