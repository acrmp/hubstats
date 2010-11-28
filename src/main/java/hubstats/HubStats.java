package hubstats;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.classifier.bayes.XmlInputFormat; // From mahout-examples

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Push events per repository from the GitHub public timeline.
 * <p/>
 * Using Mahout over Hadoop's built-in stream xml support as people seem to have had problems with it:
 *
 * @see <a href="http://oobaloo.co.uk/articles/2010/1/20/processing-xml-in-hadoop.html">Processing XML in Hadoop</a>
 */
public class HubStats extends Configured implements Tool {

    public static class PushEventMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private static final XMLInputFactory factory = XMLInputFactory.newFactory();
        private static final Pattern ID_PATTERN = Pattern.compile("^.*PushEvent/([0-9]+)$");
        private static final Pattern REPO_PATTERN = Pattern.compile("^.*at ([^/]+/.*)$");
        private static final LongWritable ONE = new LongWritable(1L);

        /**
         * Parse the feed xml and extract the push event id and repository name
         * @param key The offset of the XML feed within the larger timeline
         * @param value The XML feed text
         * @param context The job context
         * @throws IOException If there is an exception reading or writing data
         * @throws InterruptedException If this job is interrupted
         */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString().trim().length() == 0) {
                return;
            }

            try {
                XMLStreamReader sr = factory.createXMLStreamReader(new StringReader(value.toString()));

                Matcher m;
                LongWritable id = new LongWritable();
                Text repoName = new Text();
                boolean doMatch = false;

                for (int event = sr.next(); event != XMLStreamConstants.END_DOCUMENT; event = sr.next()) {
                    if (event == XMLStreamConstants.START_ELEMENT) {
                        if (sr.getLocalName().equals("id")) {
                            m = ID_PATTERN.matcher(sr.getElementText());
                            if (m.matches()) {
                                id.set(Long.parseLong(m.group(1)));
                                doMatch = true;
                            }
                        } else if (doMatch && sr.getLocalName().equals("title")) {
                            m = REPO_PATTERN.matcher(sr.getElementText());
                            if (m.matches()) {
                                repoName.set(m.group(1));
                                context.write(repoName, ONE);
                            }
                            doMatch = false;
                        }
                    }
                }
            }
            catch (XMLStreamException xse) {
                xse.printStackTrace(System.err);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job();
        job.setJarByClass(HubStats.class);
        job.setJobName("pushevents");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapperClass(PushEventMapper.class);
        job.setReducerClass(LongSumReducer.class);

        job.setInputFormatClass(XmlInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.getConfiguration().set(XmlInputFormat.START_TAG_KEY, "<feed");
        job.getConfiguration().set(XmlInputFormat.END_TAG_KEY, "feed>");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new HubStats(), args);
    }

}