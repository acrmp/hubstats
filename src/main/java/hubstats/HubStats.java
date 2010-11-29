package hubstats;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.classifier.bayes.XmlInputFormat;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

    private static final List<EventExtractor> EVENT_EXTRACTORS = Lists.newArrayList(
            new IssueExtractor(),
            new PushExtractor(),
            new CreateExtractor(),
            new WatchExtractor(),
            new MemberExtractor(),
            new ForkExtractor(),
            new ForkApplyExtractor(),
            new PublicExtractor(),
            new GollumExtractor(),
            new DeleteExtractor(),
            new DownloadExtractor(),
            new FollowExtractor(),
            new GistExtractor(),
            new PullRequestExtractor(),
            new CommitCommentExtractor()
    );

    private static final Map<EventType, EventExtractor> TYPE_EXTRACTOR_MAP =
            Maps.newHashMapWithExpectedSize(EVENT_EXTRACTORS.size());

    static {
        for (EventExtractor e : EVENT_EXTRACTORS) {
            TYPE_EXTRACTOR_MAP.put(e.getEventType(), e);
        }
    }

    private static final XMLInputFactory INPUT_FACTORY = XMLInputFactory.newFactory();
    static final Pattern ID_PATTERN = Pattern.compile("^.*:([A-Za-z]+)Event/([0-9]+)$");
    static final Pattern ISSUES_PATTERN = Pattern.compile("^([^ ]+) ([^ ]+) issue ([0-9]+) on ([^/]+)/(.*)$");
    static final Pattern PUSH_PATTERN = Pattern.compile("^([^ ]+) pushed to ([^ ]+) at ([^/]+)/(.*)$");
    static final Pattern CREATE_BRANCH_PATTERN = Pattern.compile("^([^ ]+) created (branch|tag) ([^ ]+) at ([^/]+)/(.*)$");
    static final Pattern CREATE_REPO_PATTERN = Pattern.compile("^([^ ]+) created repository (.*)$");
    static final Pattern WATCH_PATTERN = Pattern.compile("^([^ ]+) started watching ([^/]+)/(.*)$");
    static final Pattern MEMBER_PATTERN = Pattern.compile("^([^ ]+) ([^ ]+) ([^ ]+) to (.*)$");
    static final Pattern FORK_PATTERN = Pattern.compile("^([^ ]+) forked ([^/]+)/(.*)$");
    static final Pattern PUBLIC_PATTERN = Pattern.compile("^([^ ]+).* ([^ ]+)$");
    static final Pattern GOLLUM_PATTERN = Pattern.compile("^([^ ]+) ([^ ]+) a page in the ([^/]+)/(.*)$");
    static final Pattern DELETE_PATTERN = Pattern.compile("^([^ ]+) deleted (branch|tag) ([^ ]+) at (.*)$");
    static final Pattern DOWNLOAD_PATTERN = Pattern.compile("^([^ ]+) ([^ ]+) a file to ([^/]+)/(.*)$");
    static final Pattern FOLLOW_PATTERN = Pattern.compile("^([^ ]+) started following (.*)$");
    static final Pattern GIST_PATTERN = Pattern.compile("^([^ ]+) ([^ ]+) gist: ([0-9]+)$");
    static final Pattern PULLREQ_PATTERN = Pattern.compile("^([^ ]+) ([^ ]+) pull request ([0-9]+) on ([^/]+)/(.*)$");
    static final Pattern COMMENT_PATTERN = Pattern.compile("^([^ ]+) commented on ([^/]+)/(.*)$");
    static final Pattern FORK_APPLY_PATTERN = Pattern.compile("^([^ ]+) applied fork commits to ([^/]+)/(.*)$");

    public static final class EventMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        /**
         * Parse the feed xml and extract the push event id and repository name
         *
         * @param key     The offset of the XML feed within the larger timeline
         * @param value   The XML feed text
         * @param context The job context
         * @throws IOException          If there is an exception reading or writing data
         * @throws InterruptedException If this job is interrupted
         */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString().trim().length() == 0) {
                return;
            }

            try {
                XMLStreamReader sr = INPUT_FACTORY.createXMLStreamReader(new StringReader(value.toString()));

                LongWritable id = new LongWritable();
                Text eventText = new Text();

                Event.Builder builder = null;
                for (int event = sr.next(); event != XMLStreamConstants.END_DOCUMENT; event = sr.next()) {
                    if (event == XMLStreamConstants.START_ELEMENT) {
                        if (sr.getLocalName().equals("entry")) {
                            builder = new Event.Builder();
                        } else if (builder == null) {
                            continue;
                        } else if (sr.getLocalName().equals("id")) {
                            Matcher m = ID_PATTERN.matcher(sr.getElementText());
                            if (m.matches()) {
                                builder.type(EventType.valueOf(m.group(1)));
                                long eventId = Long.parseLong(m.group(2));
                                id.set(eventId);
                                builder.eventId(eventId);
                            }
                        } else if (sr.getLocalName().equals("published")) {
                            builder.at(sr.getElementText());
                        } else if (sr.getLocalName().equals("title")) {
                            boolean hasEvent = TYPE_EXTRACTOR_MAP.get(builder.getType()).extract(sr.getElementText(), builder);
                            if (!hasEvent) {
                                throw new IllegalStateException(String.format("Event not matched: %s", builder.getType()));
                            }
                        }
                    } else if (event == XMLStreamConstants.END_ELEMENT && sr.getLocalName().equals("entry")) {
                        assert builder != null;
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

    static final class IssueExtractor implements EventExtractor {

        public EventType getEventType() {
            return EventType.Issues;
        }

        public boolean extract(String text, Event.Builder builder) {
            Matcher m = HubStats.ISSUES_PATTERN.matcher(text);
            if (m.matches()) {
                builder.actor(m.group(1));
                builder.subType(m.group(2));
                builder.alternateId(Long.parseLong(m.group(3)));
                builder.repoAccount(m.group(4));
                builder.repoName(m.group(5));
            }
            return m.matches();
        }
    }

    static final class PushExtractor implements EventExtractor {

        public EventType getEventType() {
            return EventType.Push;
        }

        public boolean extract(String text, Event.Builder builder) {
            Matcher m = HubStats.PUSH_PATTERN.matcher(text);
            if (m.matches()) {
                builder.actor(m.group(1));
                builder.branch(m.group(2));
                builder.repoAccount(m.group(3));
                builder.repoName(m.group(4));
            }
            return m.matches();
        }
    }

    static final class CreateExtractor implements EventExtractor {

        public EventType getEventType() {
            return EventType.Create;
        }

        public boolean extract(String text, Event.Builder builder) {
            Matcher m = CREATE_BRANCH_PATTERN.matcher(text);
            if (m.matches()) {
                builder.actor(m.group(1));
                if (m.group(2).equals("tag")) {
                    builder.tag(m.group(3));
                } else {
                    builder.branch(m.group(3));
                }
                builder.repoAccount(m.group(4));
                builder.repoName(m.group(5));
            } else {
                m = CREATE_REPO_PATTERN.matcher(text);
                if (m.matches()) {
                    builder.actor(m.group(1));
                    builder.repoAccount(m.group(1));
                    builder.repoName(m.group(2));
                }
            }
            return m.matches();
        }
    }

    static final class WatchExtractor implements EventExtractor {

        public EventType getEventType() {
            return EventType.Watch;
        }

        public boolean extract(String text, Event.Builder builder) {
            Matcher m = HubStats.WATCH_PATTERN.matcher(text);
            if (m.matches()) {
                builder.actor(m.group(1));
                builder.repoAccount(m.group(2));
                builder.repoName(m.group(3));
            }
            return m.matches();
        }
    }

    static final class MemberExtractor implements EventExtractor {

        public EventType getEventType() {
            return EventType.Member;
        }

        public boolean extract(String text, Event.Builder builder) {
            Matcher m = MEMBER_PATTERN.matcher(text);
            if (m.matches()) {
                builder.repoAccount(m.group(1));
                builder.subType(m.group(2));
                builder.actor(m.group(3));
                builder.repoName(m.group(4));
            }
            return m.matches();
        }
    }

    static final class ForkExtractor implements EventExtractor {

        public EventType getEventType() {
            return EventType.Fork;
        }

        public boolean extract(String text, Event.Builder builder) {
            Matcher m = FORK_PATTERN.matcher(text);
            if (m.matches()) {
                builder.actor(m.group(1));
                builder.repoAccount(m.group(2));
                builder.repoName(m.group(3));
            }
            return m.matches();
        }
    }

    static final class ForkApplyExtractor implements EventExtractor {

        public EventType getEventType() {
            return EventType.ForkApply;
        }

        public boolean extract(String text, Event.Builder builder) {
            Matcher m = FORK_APPLY_PATTERN.matcher(text);
            if (m.matches()) {
                builder.actor(m.group(1));
                builder.repoAccount(m.group(2));
                builder.repoName(m.group(3));
            }
            return m.matches();
        }
    }

    static final class PublicExtractor implements EventExtractor {

        public EventType getEventType() {
            return EventType.Public;
        }

        public boolean extract(String text, Event.Builder builder) {
            Matcher m = PUBLIC_PATTERN.matcher(text);
            if (m.matches()) {
                builder.actor(m.group(1));
                builder.repoAccount(m.group(1));
                builder.repoName(m.group(2));
            }
            return m.matches();
        }
    }

    static final class GollumExtractor implements EventExtractor {

        public EventType getEventType() {
            return EventType.Gollum;
        }

        public boolean extract(String text, Event.Builder builder) {
            Matcher m = GOLLUM_PATTERN.matcher(text);
            if (m.matches()) {
                builder.actor(m.group(1));
                builder.subType(m.group(2));
                builder.repoAccount(m.group(3));
                builder.repoName(m.group(4));
            }
            return m.matches();
        }
    }

    static final class DeleteExtractor implements EventExtractor {

        public EventType getEventType() {
            return EventType.Delete;
        }

        public boolean extract(String text, Event.Builder builder) {
            Matcher m = DELETE_PATTERN.matcher(text);
            if (m.matches()) {
                builder.actor(m.group(1));
                builder.repoAccount(m.group(1));
                if (m.group(2).equals("tag")) {
                    builder.tag(m.group(3));
                } else {
                    builder.branch(m.group(3));
                }
                builder.repoName(m.group(4));
            }
            return m.matches();
        }
    }

    static final class DownloadExtractor implements EventExtractor {

        public EventType getEventType() {
            return EventType.Download;
        }

        public boolean extract(String text, Event.Builder builder) {
            Matcher m = DOWNLOAD_PATTERN.matcher(text);
            if (m.matches()) {
                builder.actor(m.group(1));
                builder.subType(m.group(2));
                builder.repoAccount(m.group(3));
                builder.repoName(m.group(4));
            }
            return m.matches();
        }
    }

    static final class FollowExtractor implements EventExtractor {

        public EventType getEventType() {
            return EventType.Follow;
        }

        public boolean extract(String text, Event.Builder builder) {
            Matcher m = FOLLOW_PATTERN.matcher(text);
            if (m.matches()) {
                builder.actor(m.group(1));
                builder.repoAccount(m.group(2));
            }
            return m.matches();
        }
    }

    static final class GistExtractor implements EventExtractor {

        public EventType getEventType() {
            return EventType.Gist;
        }

        public boolean extract(String text, Event.Builder builder) {
            Matcher m = GIST_PATTERN.matcher(text);
            if (m.matches()) {
                builder.actor(m.group(1));
                builder.subType(m.group(2));
                builder.alternateId(Long.parseLong(m.group(3)));
            }
            return m.matches();
        }
    }

    static final class PullRequestExtractor implements EventExtractor {

        public EventType getEventType() {
            return EventType.PullRequest;
        }

        public boolean extract(String text, Event.Builder builder) {
            Matcher m = PULLREQ_PATTERN.matcher(text);
            if (m.matches()) {
                builder.actor(m.group(1));
                builder.subType(m.group(2));
                builder.alternateId(Long.parseLong(m.group(3)));
                builder.repoAccount(m.group(4));
                builder.repoName(m.group(5));
            }
            return m.matches();
        }
    }

    static final class CommitCommentExtractor implements EventExtractor {

        public EventType getEventType() {
            return EventType.CommitComment;
        }

        public boolean extract(String text, Event.Builder builder) {
            Matcher m = COMMENT_PATTERN.matcher(text);
            if (m.matches()) {
                builder.actor(m.group(1));
                builder.repoAccount(m.group(2));
                builder.repoName(m.group(3));
            }
            return m.matches();
        }
    }

    /**
     * Remove duplicate events
     *
     * @param <LongWritable> The event id
     * @param <Text> The tab-separated field values
     */
    public static final class EventReducer<LongWritable, Text> extends Reducer<LongWritable, Text, LongWritable, Text> {
        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Iterator<Text> i = values.iterator();
            if (i.hasNext()) {
                context.write(key, i.next());
            }
        }
    }

    @Override
    public final int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = new Job();
        job.setJarByClass(HubStats.class);
        job.setJobName("hubstats");
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(EventMapper.class);
        job.setReducerClass(EventReducer.class);

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