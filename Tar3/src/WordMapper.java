import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordMapper extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        System.out.println("value: " + value.toString());
        if (value.getLength() > 0) {
            //sb.append(value.toString().charAt(0));
            context.write(new Text(String.valueOf(value.getLength()) + value.toString().charAt(value.getLength() - 1)),
                          value);
        }
    }
}