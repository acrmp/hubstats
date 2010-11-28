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

public class HubStatsTest {

    private static final List<Pair<LongWritable, Text>> EMPTY =
            Collections.unmodifiableList(new ArrayList<Pair<LongWritable, Text>>());
    private static final LongWritable L_ZERO = new LongWritable(0);

    private MapDriver<LongWritable, Text, LongWritable, Text> driver;

    @Before
    public void setUp() {
        Mapper<LongWritable, Text, LongWritable, Text> mapper = new HubStats.PushEventMapper();
        driver = new MapDriver<LongWritable, Text, LongWritable, Text>(mapper);
    }

    @Test
    public void empty() throws IOException {
        List<Pair<LongWritable, Text>> out = driver.withInput(L_ZERO, new Text("")).run();
        assertListEquals(EMPTY, out);
    }

    @Test
    public void realFeed() throws IOException {
        File singleFeed = new File(String.format("src%stest%sresources%ssingle-feed.log",
                File.separator, File.separator, File.separator));
        List<Pair<LongWritable, Text>> out =
                driver.withInput(L_ZERO, new Text(FileUtils.readFileToString(singleFeed))).run();

        List<Pair<LongWritable, Text>> expected = Lists.newArrayList(
                new Pair<LongWritable, Text>(new LongWritable(1007541709L), new Text(
                    new Event.Builder(1007541709L, EventType.Issues, "2010-11-19T03:55:54-08:00", "weisserd")
                        .repoAccount("weisserd").repoName("LDAP-Sync").alternateId(1L).subType("closed").build().toString()
                )),
                new Pair<LongWritable, Text> (new LongWritable(1007541708L), new Text(
                    new Event.Builder(1007541708L, EventType.Push, "2010-11-19T03:55:54-08:00", "esil")
                        .repoAccount("esil").repoName("cmake").branch("master").build().toString()
                )),
                new Pair<LongWritable, Text> (new LongWritable(1007541704L), new Text(
                    new Event.Builder(1007541704L, EventType.Create, "2010-11-19T03:55:53-08:00", "74hc595")
                        .repoAccount("74hc595").repoName("remona").branch("master").build().toString()
                )),
                new Pair<LongWritable, Text> (new LongWritable(1007541703L), new Text(
                    new Event.Builder(1007541703L, EventType.Push, "2010-11-19T03:55:53-08:00", "myitcrm")
                        .repoAccount("myitcrm").repoName("Development").branch("master").build().toString()
                )),
                new Pair<LongWritable, Text> (new LongWritable(1007541678L), new Text(
                    new Event.Builder(1007541678L, EventType.Watch, "2010-11-19T03:55:45-08:00", "marco-arnold")
                        .repoAccount("icefox").repoName("arora").build().toString()
                )),
                new Pair<LongWritable, Text> (new LongWritable(1007541668L), new Text(
                    new Event.Builder(1007541668L, EventType.Watch, "2010-11-19T03:55:44-08:00", "krawaller")
                        .repoAccount("0xfe").repoName("vexflow").build().toString()
                )),
                new Pair<LongWritable, Text> (new LongWritable(1007541666L), new Text(
                    new Event.Builder(1007541666L, EventType.Member, "2010-11-19T03:55:43-08:00", "Haderson")
                        .repoAccount("fernandopereira").repoName("valter_rails").subType("added").build().toString()
                )),
                new Pair<LongWritable, Text> (new LongWritable(1007541653L), new Text(
                    new Event.Builder(1007541653L, EventType.Push, "2010-11-19T03:55:38-08:00", "hutchike")
                        .repoAccount("hutchike").repoName("YAWF").branch("master").build().toString()
                )),
                new Pair<LongWritable, Text> (new LongWritable(1007541649L), new Text(
                    new Event.Builder(1007541649L, EventType.Push, "2010-11-19T03:55:36-08:00", "alexvasi")
                        .repoAccount("alexvasi").repoName("xapian-haystack").branch("master").build().toString()
                )),
                new Pair<LongWritable, Text> (new LongWritable(1007541648L), new Text(
                    new Event.Builder(1007541648L, EventType.Fork, "2010-11-19T03:55:35-08:00", "KeithMoss")
                        .repoAccount("arnaud").repoName("chrome-tab-sugar").build().toString()
                )),
                new Pair<LongWritable, Text> (new LongWritable(1007541632L), new Text(
                    new Event.Builder(1007541632L, EventType.Public, "2010-11-19T03:55:33-08:00", "saevarom")
                        .repoAccount("saevarom").repoName("playware").build().toString()
                )),
                new Pair<LongWritable, Text> (new LongWritable(1007541629L), new Text(
                    new Event.Builder(1007541629L, EventType.Watch, "2010-11-19T03:55:32-08:00", "marco-arnold")
                        .repoAccount("Arora").repoName("arora").build().toString()
                )),
                new Pair<LongWritable, Text> (new LongWritable(1007541619L), new Text(
                    new Event.Builder(1007541619L, EventType.Create, "2010-11-19T03:55:29-08:00", "channgo")
                        .repoAccount("channgo").repoName("SmallGift").build().toString()
                )),
                new Pair<LongWritable, Text> (new LongWritable(1007541615L), new Text(
                    new Event.Builder(1007541615L, EventType.Gollum, "2010-11-19T03:55:27-08:00", "jeroenbaas")
                        .subType("edited").repoAccount("Opstandingskerk").repoName("NWonline wiki").build().toString()
                )),
                new Pair<LongWritable, Text> (new LongWritable(1007541584L), new Text(
                    new Event.Builder(1007541584L, EventType.Push, "2010-11-19T03:55:21-08:00", "douglaspilar")
                        .repoAccount("natanielschling").repoName("todeschini").branch("master").build().toString()
                )),
                new Pair<LongWritable, Text> (new LongWritable(1007541561L), new Text(
                    new Event.Builder(1007541561L, EventType.Watch, "2010-11-19T03:55:18-08:00", "mengu")
                        .repoAccount("kbhomes").repoName("TextCaptchaBreaker").build().toString()
                )),
                new Pair<LongWritable, Text> (new LongWritable(1007541560L), new Text(
                    new Event.Builder(1007541560L, EventType.Create, "2010-11-19T03:55:18-08:00", "marcomaggi")
                        .repoAccount("marcomaggi").repoName("Infix").tag("1.0.3").build().toString()
                )),
                new Pair<LongWritable, Text> (new LongWritable(1007541559L), new Text(
                    new Event.Builder(1007541559L, EventType.Push, "2010-11-19T03:55:17-08:00", "marcomaggi")
                        .repoAccount("marcomaggi").repoName("Infix").branch("master").build().toString()
                )),
                new Pair<LongWritable, Text> (new LongWritable(1007541537L), new Text(
                    new Event.Builder(1007541537L, EventType.Fork, "2010-11-19T03:55:13-08:00", "mikker")
                        .repoAccount("magicalpanda").repoName("activerecord-fetching-for-core-data").build().toString()
                )),
                new Pair<LongWritable, Text> (new LongWritable(1007541536L), new Text(
                    new Event.Builder(1007541536L, EventType.Push, "2010-11-19T03:55:11-08:00", "dsyer")
                        .repoAccount("SpringSource").repoName("spring-batch-admin").branch("master").build().toString()
                )),
                new Pair<LongWritable, Text> (new LongWritable(1007541502L), new Text(
                    new Event.Builder(1007541502L, EventType.Delete, "2010-11-19T03:55:09-08:00", "clbustos")
                        .repoAccount("clbustos").repoName("rubyvis").branch("stack_layout").build().toString()
                )),
                new Pair<LongWritable, Text> (new LongWritable(1007541501L), new Text(
                    new Event.Builder(1007541501L, EventType.Push, "2010-11-19T03:55:09-08:00", "Punksoft")
                        .repoAccount("Evil-Code").repoName("Evil-Co.de-Projectdatabase").branch("master").build().toString()
                )),
                new Pair<LongWritable, Text> (new LongWritable(1007541480L), new Text(
                    new Event.Builder(1007541480L, EventType.Push, "2010-11-19T03:55:07-08:00", "gfunkmonk")
                        .repoAccount("gfunkmonk").repoName("android_packages_apps_Camera").branch("froyo").build().toString()
                )),
                new Pair<LongWritable, Text> (new LongWritable(1007541479L), new Text(
                    new Event.Builder(1007541479L, EventType.Push, "2010-11-19T03:55:07-08:00", "trsilva")
                        .repoAccount("trsilva").repoName("ForCA").branch("master").build().toString()
                )),
                new Pair<LongWritable, Text> (new LongWritable(1007541464L), new Text(
                    new Event.Builder(1007541464L, EventType.Download, "2010-11-19T03:55:04-08:00", "Duny")
                        .repoAccount("Duny").subType("uploaded").repoName("foo_my_autoplaylist").build().toString()
                )),
                new Pair<LongWritable, Text> (new LongWritable(1007541463L), new Text(
                    new Event.Builder(1007541463L, EventType.Push, "2010-11-19T03:55:02-08:00", "certik")
                        .repoAccount("certik").repoName("hermes").branch("master").build().toString()
                )),
                new Pair<LongWritable, Text> (new LongWritable(1007541462L), new Text(
                    new Event.Builder(1007541462L, EventType.Push, "2010-11-19T03:55:01-08:00", "certik")
                        .repoAccount("certik").repoName("hermes").branch("fullerene").build().toString()
                )),
                new Pair<LongWritable, Text> (new LongWritable(1007541459L), new Text(
                    new Event.Builder(1007541459L, EventType.Watch, "2010-11-19T03:55:00-08:00", "chrisdew")
                        .repoAccount("garycourt").repoName("JSV").build().toString()
                )),
                new Pair<LongWritable, Text> (new LongWritable(1007541426L), new Text(
                    new Event.Builder(1007541426L, EventType.Delete, "2010-11-19T03:54:57-08:00", "clbustos")
                        .repoAccount("clbustos").repoName("rubyvis").branch("network_rem").build().toString()
                )),
                new Pair<LongWritable, Text> (new LongWritable(1007541425L), new Text(
                    new Event.Builder(1007541425L, EventType.Push, "2010-11-19T03:54:56-08:00", "vp1981")
                        .repoAccount("vp1981").repoName("pkgbuild").branch("master").build().toString()
                ))
        );
        assertListEquals(expected, out);
    }

}

