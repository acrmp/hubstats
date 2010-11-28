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
 * Process events from the GitHub public timeline.
 * <p/>
 * Using Mahout over Hadoop's built-in stream xml support as people seem to have had problems with it:
 *
 * @see <a href="http://oobaloo.co.uk/articles/2010/1/20/processing-xml-in-hadoop.html">Processing XML in Hadoop</a>
 */
public class HubStats extends Configured implements Tool {

    public static class PushEventMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        private static final XMLInputFactory factory = XMLInputFactory.newFactory();
        private static final Pattern ID_PATTERN = Pattern.compile("^.*:([A-Za-z]+)Event/([0-9]+)$");
        private static final Pattern ISSUES_PATTERN = Pattern.compile("^([^ ]+) ([^ ]+) issue ([0-9]+) on ([^/]+)/(.*)$");
        private static final Pattern PUSH_PATTERN = Pattern.compile("^([^ ]+) pushed to ([^ ]+) at ([^/]+)/(.*)$");
        private static final Pattern CREATE_BRANCH_PATTERN = Pattern.compile("^([^ ]+) created (branch|tag) ([^ ]+) at ([^/]+)/(.*)$");
        private static final Pattern CREATE_REPO_PATTERN = Pattern.compile("^([^ ]+) created repository (.*)$");
        private static final Pattern WATCH_PATTERN = Pattern.compile("^([^ ]+) started watching ([^/]+)/(.*)$");
        private static final Pattern MEMBER_PATTERN = Pattern.compile("^([^ ]+) ([^ ]+) ([^ ]+) to (.*)$");
        private static final Pattern FORK_PATTERN = Pattern.compile("^([^ ]+) forked ([^/]+)/(.*)$");
        private static final Pattern PUBLIC_PATTERN = Pattern.compile("^([^ ]+).* ([^ ]+)$");
        private static final Pattern GOLLUM_PATTERN = Pattern.compile("^([^ ]+) ([^ ]+) a page in the ([^/]+)/(.*)$");
        private static final Pattern DELETE_PATTERN = Pattern.compile("^([^ ]+) deleted (branch|tag) ([^ ]+) at (.*)$");
        private static final Pattern DOWNLOAD_PATTERN = Pattern.compile("^([^ ]+) ([^ ]+) a file to ([^/]+)/(.*)$");
        private static final Pattern FOLLOW_PATTERN = Pattern.compile("^([^ ]+) started following (.*)$");
        private static final Pattern GIST_PATTERN = Pattern.compile("^([^ ]+) ([^ ]+) gist: ([0-9]+)$");
        private static final Pattern PULLREQ_PATTERN = Pattern.compile("^([^ ]+) ([^ ]+) pull request ([0-9]+) on ([^/]+)/(.*)$");
        private static final Pattern COMMENT_PATTERN = Pattern.compile("^([^ ]+) commented on ([^/]+)/(.*)$");

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
                Text eventText = new Text();

                Event.Builder builder = null;
                for (int event = sr.next(); event != XMLStreamConstants.END_DOCUMENT; event = sr.next()) {
                    if (event == XMLStreamConstants.START_ELEMENT) {
                        if (sr.getLocalName().equals("entry")) {
                            builder = new Event.Builder();   
                        }
                        else if (builder == null) {
                            continue;
                        }
                        else if (sr.getLocalName().equals("id")) {
                            m = ID_PATTERN.matcher(sr.getElementText());
                            if (m.matches()) {
                                builder.type(EventType.valueOf(m.group(1)));
                                long eventId = Long.parseLong(m.group(2));
                                id.set(eventId);
                                builder.eventId(eventId);
                            }
                        }
                        else if (sr.getLocalName().equals("published")) {
                            builder.at(sr.getElementText());
                        }
                        else if (sr.getLocalName().equals("title")) {
                            switch (builder.getType()) {
                                case Issues:
                                    m = ISSUES_PATTERN.matcher(sr.getElementText());
                                    if (m.matches()) {
                                        builder.actor(m.group(1));
                                        builder.subType(m.group(2));
                                        builder.alternateId(Long.parseLong(m.group(3)));
                                        builder.repoAccount(m.group(4));
                                        builder.repoName(m.group(5));
                                    }
                                    break;
                                case Push:
                                    m = PUSH_PATTERN.matcher(sr.getElementText());
                                    if (m.matches()) {
                                        builder.actor(m.group(1));
                                        builder.branch(m.group(2));
                                        builder.repoAccount(m.group(3));
                                        builder.repoName(m.group(4));
                                    }                                   
                                    break;
                                case Create:
                                    String text = sr.getElementText();
                                    m = CREATE_BRANCH_PATTERN.matcher(text);
                                    if (m.matches()) {
                                        builder.actor(m.group(1));
                                        if (m.group(2).equals("tag")) {
                                            builder.tag(m.group(3));
                                        }
                                        else {
                                            builder.branch(m.group(3));
                                        }
                                        builder.repoAccount(m.group(4));
                                        builder.repoName(m.group(5));
                                    }
                                    else {
                                        m = CREATE_REPO_PATTERN.matcher(text);
                                        if (m.matches()) {
                                            builder.actor(m.group(1));
                                            builder.repoAccount(m.group(1));
                                            builder.repoName(m.group(2));
                                        }
                                    }
                                    break;
                                case Watch:
                                    m = WATCH_PATTERN.matcher(sr.getElementText());
                                    if (m.matches()) {
                                        builder.actor(m.group(1));
                                        builder.repoAccount(m.group(2));
                                        builder.repoName(m.group(3));
                                    }
                                    break;
                                case Member:
                                    m = MEMBER_PATTERN.matcher(sr.getElementText());
                                    if (m.matches()) {
                                        builder.repoAccount(m.group(1));
                                        builder.subType(m.group(2));
                                        builder.actor(m.group(3));
                                        builder.repoName(m.group(4));
                                    }
                                    break;
                                case Fork:
                                    m = FORK_PATTERN.matcher(sr.getElementText());
                                    if (m.matches()) {
                                        builder.actor(m.group(1));
                                        builder.repoAccount(m.group(2));
                                        builder.repoName(m.group(3));
                                    }
                                    break;
                                case Public:
                                    m = PUBLIC_PATTERN.matcher(sr.getElementText());
                                    if (m.matches()) {
                                        builder.actor(m.group(1));
                                        builder.repoAccount(m.group(1));
                                        builder.repoName(m.group(2));
                                    }
                                    break;
                                case Gollum:
                                    m = GOLLUM_PATTERN.matcher(sr.getElementText());
                                    if (m.matches()) {
                                        builder.actor(m.group(1));
                                        builder.subType(m.group(2));
                                        builder.repoAccount(m.group(3));
                                        builder.repoName(m.group(4));
                                    }
                                    break;
                                case Delete:
                                    m = DELETE_PATTERN.matcher(sr.getElementText());
                                    if (m.matches()) {
                                        builder.actor(m.group(1));
                                        builder.repoAccount(m.group(1));
                                        if (m.group(2).equals("tag")) {
                                            builder.tag(m.group(3));    
                                        }
                                        else {
                                            builder.branch(m.group(3));
                                        }
                                        builder.repoName(m.group(4));
                                    }
                                    break;
                                case Download:
                                    m = DOWNLOAD_PATTERN.matcher(sr.getElementText());
                                    if (m.matches()) {
                                        builder.actor(m.group(1));
                                        builder.subType(m.group(2));
                                        builder.repoAccount(m.group(3));
                                        builder.repoName(m.group(4));
                                    }
                                    break;
                                case Follow:
                                    m = FOLLOW_PATTERN.matcher(sr.getElementText());
                                    if (m.matches()) {
                                        builder.actor(m.group(1));
                                        builder.repoAccount(m.group(2));
                                    }
                                    break;
                                case Gist:
                                    m = GIST_PATTERN.matcher(sr.getElementText());
                                    if (m.matches()) {
                                        builder.actor(m.group(1));
                                        builder.subType(m.group(2));
                                        builder.alternateId(Long.parseLong(m.group(3)));
                                    }
                                    break;
                                case PullRequest:
                                    m = PULLREQ_PATTERN.matcher(sr.getElementText());
                                    if (m.matches()) {
                                        builder.actor(m.group(1));
                                        builder.subType(m.group(2));
                                        builder.alternateId(Long.parseLong(m.group(3)));
                                        builder.repoAccount(m.group(4));
                                        builder.repoName(m.group(5));
                                    }
                                    break;
                                case CommitComment:
                                    m = COMMENT_PATTERN.matcher(sr.getElementText());
                                    if (m.matches()) {
                                        builder.actor(m.group(1));
                                        builder.repoAccount(m.group(2));
                                        builder.repoName(m.group(3));
                                    }
                                    break;
                                default:
                                    throw new IllegalStateException("Event type not recognised: " + builder.getType());
                            }
                        }
                    }
                    else if (event == XMLStreamConstants.END_ELEMENT && sr.getLocalName().equals("entry")) {
                        eventText.set(builder.build().toString());
                        context.write(id, eventText);
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
        job.setJobName("hubstats");
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(PushEventMapper.class);

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