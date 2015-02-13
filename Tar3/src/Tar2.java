import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class Tar2 {

	// fix the bug in the master r!!!
	// fix the bug in the master r!!!
	// fix the bug in the master r!!!
	// fix the bug in the master r!!!
	
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        //send the filter text path
        conf.set("filter_path", args[1]);

        Job job = new Job(conf, "Tar2");
        job.setJarByClass(Tar2.class);
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(MagicReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
