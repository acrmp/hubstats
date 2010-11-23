package hubstats;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.mrunit.testutil.ExtendedAssert.assertListEquals;

public class PushEventsTest {

    private static final List<Pair<LongWritable, Text>> EMPTY =
            Collections.unmodifiableList(new ArrayList<Pair<LongWritable, Text>>());
    private static final LongWritable L_ZERO = new LongWritable(0);
    private static final LongWritable ONE = new LongWritable(1);

    private MapDriver<LongWritable, Text, Text, LongWritable> driver;

    @Before
    public void setUp() {
        Mapper<LongWritable, Text, Text, LongWritable> mapper = new PushEvents.PushEventMapper();
        driver = new MapDriver<LongWritable, Text, Text, LongWritable>(mapper);
    }

    @Test
    public void empty() throws IOException {
        List<Pair<Text, LongWritable>> out = driver.withInput(L_ZERO, new Text("")).run();
        assertListEquals(EMPTY, out);
    }

    @Test
    public void singleFeed() throws IOException {
        File singleFeed = new File(String.format("src%stest%sresources%ssingle-feed.log",
                File.separator, File.separator, File.separator));
        List<Pair<Text, LongWritable>> out =
                driver.withInput(L_ZERO, new Text(FileUtils.readFileToString(singleFeed))).run();
        List<Pair<Text, LongWritable>> expected = Lists.newArrayList(
                new Pair<Text, LongWritable>(new Text("esil/cmake"), ONE),
                new Pair<Text, LongWritable>(new Text("myitcrm/Development"), ONE),
                new Pair<Text, LongWritable>(new Text("hutchike/YAWF"), ONE),
                new Pair<Text, LongWritable>(new Text("alexvasi/xapian-haystack"), ONE),
                new Pair<Text, LongWritable>(new Text("natanielschling/todeschini"), ONE),
                new Pair<Text, LongWritable>(new Text("marcomaggi/Infix"), ONE),
                new Pair<Text, LongWritable>(new Text("SpringSource/spring-batch-admin"), ONE),
                new Pair<Text, LongWritable>(new Text("Evil-Code/Evil-Co.de-Projectdatabase"), ONE),
                new Pair<Text, LongWritable>(new Text("gfunkmonk/android_packages_apps_Camera"), ONE),
                new Pair<Text, LongWritable>(new Text("trsilva/ForCA"), ONE),
                new Pair<Text, LongWritable>(new Text("certik/hermes"), ONE),
                new Pair<Text, LongWritable>(new Text("certik/hermes"), ONE),
                new Pair<Text, LongWritable>(new Text("vp1981/pkgbuild"), ONE)
        );
        assertListEquals(expected, out);
    }

}

