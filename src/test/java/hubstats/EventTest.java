package hubstats;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

//TODO: Ensure validation exceptions provide id of problem event in the message
public class EventTest {

    @Test
    public void eventIdZero() {
        try {
            new Event.Builder(0L, EventType.valueOf("Push"), "2010-11-19T03:55:54-08:00", "esil").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Event id must be greater than zero (was 0)", e.getMessage());
        }
    }

    @Test
    public void eventIdLessThanZero() {
        try {
            new Event.Builder(-1L, EventType.valueOf("Push"), "2010-11-19T03:55:54-08:00", "esil").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Event id must be greater than zero (was -1)", e.getMessage());
        }
    }

    @Test
    public void unknownEventType() {
        try {
            new Event.Builder(1234L, EventType.valueOf("MonkeyPatch"), "2010-11-19T03:55:54-08:00", "esil").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("No enum const class hubstats.EventType.MonkeyPatch", e.getMessage());
        }
    }

    @Test
    @Ignore
    public void invalidDateFormat() {
    }

    @Test
    @Ignore
    public void invalidDate() {
    }

    @Test
    @Ignore
    public void invalidTime() {
    }

    @Test
    public void nullActor() {
        try {
            new Event.Builder(1007541708L, EventType.valueOf("Push"), "2010-11-19T03:55:54-08:00", null).build();
            fail();
        }
        catch (IllegalArgumentException npe) {
            assertEquals("Actor account name cannot be null or empty", npe.getMessage());
        }
    }

    @Test
    public void emptyActor() {
        try {
            new Event.Builder(1007541708L, EventType.valueOf("Push"), "2010-11-19T03:55:54-08:00", "").build();
            fail();
        }
        catch (IllegalArgumentException npe) {
            assertEquals("Actor account name cannot be null or empty", npe.getMessage());
        }
    }

    @Test
    public void paddedActor() {
        try {
            new Event.Builder(1007541708L, EventType.valueOf("Push"), "2010-11-19T03:55:54-08:00", " \t").build();
            fail();
        }
        catch (IllegalArgumentException npe) {
            assertEquals("Actor account name cannot be null or empty", npe.getMessage());
        }
    }

    @Test
    public void trimPaddedActor() {
        Event e = new Event.Builder(1007541708L, EventType.valueOf("Push"), "2010-11-19T03:55:54-08:00", " asdf \t")
                .repoAccount("foo").repoName("bar").branch("alice").build();
        assertEquals("asdf", e.getActor());
    }

    @Test
    public void pushMaster() {
        Event e = new Event.Builder(1007541708L, EventType.valueOf("Push"), "2010-11-19T03:55:54-08:00", "esil")
                .repoAccount("esil").repoName("cmake").branch("master").build();
        assertEquals("1007541708\tPush\t2010-11-19T03:55:54-08:00\tesil\tesil\tcmake\tmaster\t\t\t", e.toString());
    }

    @Test
    public void pushMustHaveRepoAccount() {
        try {
            new Event.Builder(1007541708L, EventType.valueOf("Push"), "2010-11-19T03:55:54-08:00", "esil")
                    .repoName("cmake").branch("master").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Push event must have an associated repository account", e.getMessage());
        }
    }

    @Test
    public void pushMustHaveValidRepoAccount() {
        try {
            new Event.Builder(1007541708L, EventType.valueOf("Push"), "2010-11-19T03:55:54-08:00", "esil")
                    .repoAccount("\t\t").repoName("cmake").branch("master").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Push event must have an associated repository account", e.getMessage());
        }
    }

    @Test
    public void pushTrimPaddedRepoAccount() {
        Event e = new Event.Builder(1007541708L, EventType.valueOf("Push"), "2010-11-19T03:55:54-08:00", "esil")
                .repoAccount("\tfoo\t").repoName("cmake").branch("master").build();
        assertEquals("foo", e.getRepoAccount());
    }

    @Test
    public void pushMustHaveRepoName() {
        try {
            new Event.Builder(1007541708L, EventType.valueOf("Push"), "2010-11-19T03:55:54-08:00", "esil")
                    .repoAccount("esil").branch("master").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Push event must have an associated repository name", e.getMessage());
        }
    }

    @Test
    public void pushMustHaveValidRepoName() {
        try {
            new Event.Builder(1007541708L, EventType.valueOf("Push"), "2010-11-19T03:55:54-08:00", "esil")
                    .repoAccount("esil").repoName("   ").branch("master").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Push event must have an associated repository name", e.getMessage());
        }
    }

    @Test
    public void pushTrimPaddedRepoName() {
        Event e = new Event.Builder(1007541708L, EventType.valueOf("Push"), "2010-11-19T03:55:54-08:00", "esil")
                .repoAccount("foo").repoName("  cmake").branch("master").build();
        assertEquals("cmake", e.getRepoName());
    }

    @Test
    public void pushMustHaveBranch() {
        try {
            new Event.Builder(1007541708L, EventType.valueOf("Push"), "2010-11-19T03:55:54-08:00", "esil")
                    .repoAccount("esil").repoName("cmake").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Push event must have an associated branch", e.getMessage());
        }
    }

    @Test
    public void pushMustHaveValidBranch() {
        try {
            new Event.Builder(1007541708L, EventType.valueOf("Push"), "2010-11-19T03:55:54-08:00", "esil")
                    .repoAccount("esil").repoName("foo").branch("\t   \t\t").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Push event must have an associated branch", e.getMessage());
        }
    }

    @Test
    public void pushTrimPaddedBranch() {
        Event e = new Event.Builder(1007541708L, EventType.valueOf("Push"), "2010-11-19T03:55:54-08:00", "esil")
                .repoAccount("foo").repoName("cmake").branch("master\t    \t\t").build();
        assertEquals("master", e.getBranch());
    }

    @Test
    public void pushBranch() {
        Event e = new Event.Builder(1007541480L, EventType.valueOf("Push"), "2010-11-19T03:55:07-08:00", "gfunkmonk")
                .repoAccount("gfunkmonk").repoName("android_packages_apps_Camera").branch("froyo").build();
        assertEquals("1007541480\tPush\t2010-11-19T03:55:07-08:00\tgfunkmonk\tgfunkmonk\tandroid_packages_apps_Camera\tfroyo\t\t\t", e.toString());
    }

    @Test
    public void pushAnotherRepo() {
        Event e = new Event.Builder(1007555670L, EventType.valueOf("Push"), "2010-11-19T04:03:09-08:00", "elishowk")
                .repoAccount("moma").repoName("tinasoft.desktop").branch("master").build();
        assertEquals("1007555670\tPush\t2010-11-19T04:03:09-08:00\telishowk\tmoma\ttinasoft.desktop\tmaster\t\t\t", e.toString());
    }

    @Test
    public void createBranchMaster() {
        Event e = new Event.Builder(1007541704L, EventType.valueOf("Create"), "2010-11-19T03:55:53-08:00", "74hc595")
                .repoAccount("74hc595").repoName("remona").branch("master").build();
        assertEquals("1007541704\tCreate\t2010-11-19T03:55:53-08:00\t74hc595\t74hc595\tremona\tmaster\t\t\t", e.toString());
    }

    @Test
    public void createRepo() {
        Event e = new Event.Builder(1007541619L, EventType.valueOf("Create"), "2010-11-19T03:55:29-08:00", "channgo")
                .repoAccount("channgo").repoName("SmallGift").build();
        assertEquals("1007541619\tCreate\t2010-11-19T03:55:29-08:00\tchanngo\tchanngo\tSmallGift\t\t\t\t", e.toString());
    }

    @Test
    public void createTag() {
        Event e = new Event.Builder(1007541560L, EventType.valueOf("Create"), "2010-11-19T03:55:18-08:00", "marcomaggi")
                .repoAccount("marcomaggi").repoName("Infix").tag("1.0.3").build();
        assertEquals("1007541560\tCreate\t2010-11-19T03:55:18-08:00\tmarcomaggi\tmarcomaggi\tInfix\t\t1.0.3\t\t", e.toString());
    }

    @Test
    public void createMustHaveRepoAccount() {
        try {
            new Event.Builder(1007541721L, EventType.valueOf("Create"), "2010-11-19T03:55:59-08:00", "refaim")
                    .repoName("ttb-game").branch("oop").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Create event must have an associated repository account", e.getMessage());
        }
    }

    @Test
    public void createMustHaveRepoName() {
        try {
            new Event.Builder(1007541721L, EventType.valueOf("Create"), "2010-11-19T03:55:59-08:00", "refaim")
                    .repoAccount("marcomaggi").branch("oop").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Create event must have an associated repository name", e.getMessage());
        }
    }

    @Test
    public void createBranch() {
        Event e = new Event.Builder(1007541721L, EventType.valueOf("Create"), "2010-11-19T03:55:59-08:00", "refaim")
                .repoAccount("kravitz").repoName("ttb-game").branch("oop").build();
        assertEquals("1007541721\tCreate\t2010-11-19T03:55:59-08:00\trefaim\tkravitz\tttb-game\toop\t\t\t", e.toString());
    }

    @Test
    public void branchAndTag() {
        try {
            new Event.Builder(1007541721L, EventType.valueOf("Create"), "2010-11-19T03:55:59-08:00", "refaim")
                    .repoAccount("kravitz").repoName("ttb-game").branch("oop").tag("oop2").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Can't create a branch and a tag in the same event", e.getMessage());
        }
    }

    @Test
    public void watchRepo() {
        Event e = new Event.Builder(1007541678L, EventType.valueOf("Watch"), "2010-11-19T03:55:45-08:00", "marco-arnold")
                .repoAccount("icefox").repoName("arora").build();
        assertEquals("1007541678\tWatch\t2010-11-19T03:55:45-08:00\tmarco-arnold\ticefox\tarora\t\t\t\t", e.toString());
    }

    @Test
    public void watchMustHaveRepoAccount() {
        try {
            new Event.Builder(1007541678L, EventType.valueOf("Watch"), "2010-11-19T03:55:45-08:00", "marco-arnold")
                    .repoName("arora").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Watch event must have an associated repository account", e.getMessage());
        }
    }

    @Test
    public void watchMustHaveRepoName() {
        try {
            new Event.Builder(1007541678L, EventType.valueOf("Watch"), "2010-11-19T03:55:45-08:00", "marco-arnold")
                    .repoAccount("icefox").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Watch event must have an associated repository name", e.getMessage());
        }
    }

    @Test
    public void closeIssue() {
        Event e = new Event.Builder(1007541709L, EventType.valueOf("Issues"), "2010-11-19T03:55:54-08:00", "weisserd")
                .repoAccount("weisserd").repoName("LDAP-Sync").alternateId(1L).subType("closed").build();
        assertEquals("1007541709\tIssues\t2010-11-19T03:55:54-08:00\tweisserd\tweisserd\tLDAP-Sync\t\t\t1\tclosed", e.toString());
    }

    @Test
    public void openIssue() {
        Event e = new Event.Builder(1007544980L, EventType.valueOf("Issues"), "2010-11-19T03:56:52-08:00", "grinnbearit")
                .repoAccount("technomancy").repoName("swank-clojure").alternateId(32L).subType("opened").build();
        assertEquals("1007544980\tIssues\t2010-11-19T03:56:52-08:00\tgrinnbearit\ttechnomancy\tswank-clojure\t\t\t32\topened", e.toString());
    }

    @Test
    public void issueMustHaveRepoAccount() {
        try {
            new Event.Builder(1007541709L, EventType.valueOf("Issues"), "2010-11-19T03:55:54-08:00", "weisserd")
                    .repoName("LDAP-Sync").alternateId(1L).subType("closed").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Issues event must have an associated repository account", e.getMessage());
        }
    }

    @Test
    public void issueMustHaveRepoName() {
        try {
            new Event.Builder(1007541709L, EventType.valueOf("Issues"), "2010-11-19T03:55:54-08:00", "weisserd")
                    .repoAccount("weisserd").alternateId(1L).subType("closed").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Issues event must have an associated repository name", e.getMessage());
        }
    }

    @Test
    public void issueMustSpecifyId() {
        try {
            new Event.Builder(1007544980L, EventType.valueOf("Issues"), "2010-11-19T03:56:52-08:00", "grinnbearit")
                    .repoAccount("technomancy").repoName("swank-clojure").subType("opened").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Issues event must specify the id of the issue", e.getMessage());
        }
    }

    @Test
    public void issueMustSpecifyType() {
        try {
            new Event.Builder(1007544980L, EventType.valueOf("Issues"), "2010-11-19T03:56:52-08:00", "grinnbearit")
                    .repoAccount("technomancy").repoName("swank-clojure").alternateId(1L).build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Issues event must specify the type of the operation", e.getMessage());
        }
    }

    @Test
    public void followRepo() {
        Event e = new Event.Builder(1007545371L, EventType.valueOf("Follow"), "2010-11-19T03:58:03-08:00", "vitorpc")
                .repoAccount("tkyk").build();
        assertEquals("1007545371\tFollow\t2010-11-19T03:58:03-08:00\tvitorpc\ttkyk\t\t\t\t\t", e.toString());
    }

    @Test
    public void followMustHaveRepoAccount() {
        try {
            new Event.Builder(1007545371L, EventType.valueOf("Follow"), "2010-11-19T03:58:03-08:00", "vitorpc").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Follow event must have an associated repository account", e.getMessage());
        }
    }

    @Test
    public void followMustNotHaveRepoName() {
        try {
            new Event.Builder(1007545371L, EventType.valueOf("Follow"), "2010-11-19T03:58:03-08:00", "vitorpc")
                    .repoAccount("tkyk").repoName("bar").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Follow event must not have an associated repository name", e.getMessage());
        }
    }

    @Test
    public void forkRepo() {
        Event e = new Event.Builder(1007541648L, EventType.valueOf("Fork"), "2010-11-19T03:55:35-08:00", "KeithMoss")
                .repoAccount("arnaud").repoName("chrome-tab-sugar").build();
        assertEquals("1007541648\tFork\t2010-11-19T03:55:35-08:00\tKeithMoss\tarnaud\tchrome-tab-sugar\t\t\t\t", e.toString());
    }

    @Test
    public void forkMustHaveRepoAccount() {
        try {
            new Event.Builder(1007541648L, EventType.valueOf("Fork"), "2010-11-19T03:55:35-08:00", "KeithMoss")
                    .repoName("chrome-tab-sugar").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Fork event must have an associated repository account", e.getMessage());
        }
    }

    @Test
    public void forkMustHaveRepoName() {
        try {
            new Event.Builder(1007541648L, EventType.valueOf("Fork"), "2010-11-19T03:55:35-08:00", "KeithMoss")
                    .repoAccount("arnaud").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Fork event must have an associated repository name", e.getMessage());
        }
    }

    @Test
    public void applyFork() {
        Event e = new Event.Builder(1007566139L, EventType.valueOf("ForkApply"), "2010-11-19T04:21:21-08:00", "cdotyone")
                .repoAccount("cdotyone").repoName("mootools-meio-mask").build();
        assertEquals("1007566139\tForkApply\t2010-11-19T04:21:21-08:00\tcdotyone\tcdotyone\tmootools-meio-mask\t\t\t\t",
                e.toString());
    }

    @Test
    public void forkApplyMustHaveRepoAccount() {
        try {
            new Event.Builder(1007566139L, EventType.valueOf("ForkApply"), "2010-11-19T04:21:21-08:00", "cdotyone")
                    .repoName("mootools-meio-mask").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("ForkApply event must have an associated repository account", e.getMessage());
        }
    }

    @Test
    public void forkApplyMustHaveRepoName() {
        try {
            new Event.Builder(1007566139L, EventType.valueOf("ForkApply"), "2010-11-19T04:21:21-08:00", "cdotyone")
                    .repoAccount("cdotyone").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("ForkApply event must have an associated repository name", e.getMessage());
        }
    }

    @Test
    public void updateGist() {
        Event e = new Event.Builder(1007546130L, EventType.valueOf("Gist"), "2010-11-19T03:58:11-08:00", "zakuro563")
                .alternateId(706394L).subType("updated").build();
        assertEquals("1007546130\tGist\t2010-11-19T03:58:11-08:00\tzakuro563\t\t\t\t\t706394\tupdated", e.toString());
    }

    @Test
    public void createGist() {
        Event e = new Event.Builder(1007555941L, EventType.valueOf("Gist"), "2010-11-19T04:03:51-08:00", "malditogeek")
                .alternateId(706433L).subType("created").build();
        assertEquals("1007555941\tGist\t2010-11-19T04:03:51-08:00\tmalditogeek\t\t\t\t\t706433\tcreated", e.toString());
    }

    @Test
    public void forkGist() {
        Event e = new Event.Builder(1007577244L, EventType.valueOf("Gist"), "2010-11-19T04:33:47-08:00", "tkyk")
                .alternateId(706463L).subType("forked").build();
        assertEquals("1007577244\tGist\t2010-11-19T04:33:47-08:00\ttkyk\t\t\t\t\t706463\tforked", e.toString());
    }

    @Test
    public void gistMustSpecifyId() {
        try {
            new Event.Builder(1007577244L, EventType.valueOf("Gist"), "2010-11-19T04:33:47-08:00", "tkyk")
                    .subType("forked").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Gist event must specify the id of the gist", e.getMessage());
        }
    }

    @Test
    public void gistMustSpecifyType() {
        try {
            new Event.Builder(1007577244L, EventType.valueOf("Gist"), "2010-11-19T04:33:47-08:00", "tkyk")
                    .alternateId(706463L).build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Gist event must specify the type of the operation", e.getMessage());
        }
    }

    @Test
    public void gistMustNotSpecifyRepoAccount() {
        try {
            new Event.Builder(1007577244L, EventType.valueOf("Gist"), "2010-11-19T04:33:47-08:00", "tkyk")
                    .repoAccount("foo").alternateId(706463L).subType("forked").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Gist event must not have an associated repository account", e.getMessage());
        }
    }

    @Test
    public void gistMustNotSpecifyRepoName() {
        try {
            new Event.Builder(1007577244L, EventType.valueOf("Gist"), "2010-11-19T04:33:47-08:00", "tkyk")
                    .repoName("bar").alternateId(706463L).subType("forked").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Gist event must not have an associated repository name", e.getMessage());
        }
    }


    @Test
    public void addMember() {
        Event e = new Event.Builder(1007541666L, EventType.valueOf("Member"), "2010-11-19T03:55:43-08:00", "Haderson")
                .repoAccount("fernandopereira").repoName("valter_rails").subType("added").build();
        assertEquals("1007541666\tMember\t2010-11-19T03:55:43-08:00\tHaderson\tfernandopereira\tvalter_rails\t\t\t\tadded", e.toString());
    }

    @Test
    public void memberMustHaveRepoAccount() {
        try {
            new Event.Builder(1007541666L, EventType.valueOf("Member"), "2010-11-19T03:55:43-08:00", "Haderson")
                    .repoName("valter_rails").subType("added").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Member event must have an associated repository account", e.getMessage());
        }
    }

    @Test
    public void memberMustHaveRepoName() {
        try {
            new Event.Builder(1007541666L, EventType.valueOf("Member"), "2010-11-19T03:55:43-08:00", "Haderson")
                    .repoAccount("fernandopereira").subType("added").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Member event must have an associated repository name", e.getMessage());
        }
    }

    @Test
    public void closePullRequest() {
        Event e = new Event.Builder(1007546128L, EventType.valueOf("PullRequest"), "2010-11-19T03:58:11-08:00", "defunkt")
                .repoAccount("defunkt").repoName("mustache").alternateId(64L).subType("closed").build();
        assertEquals("1007546128\tPullRequest\t2010-11-19T03:58:11-08:00\tdefunkt\tdefunkt\tmustache\t\t\t64\tclosed", e.toString());
    }

    @Test
    public void openPullRequest() {
        Event e = new Event.Builder(1007547014L, EventType.valueOf("PullRequest"), "2010-11-19T03:59:02-08:00", "jitter")
                .repoAccount("jquery").repoName("jquery").alternateId(102L).subType("opened").build();
        assertEquals("1007547014\tPullRequest\t2010-11-19T03:59:02-08:00\tjitter\tjquery\tjquery\t\t\t102\topened", e.toString());
    }

    @Test
    public void mergePullRequest() {
        Event e = new Event.Builder(1007553635L, EventType.valueOf("PullRequest"), "2010-11-19T04:00:26-08:00", "specht")
                .repoAccount("specht").repoName("proteomatic").alternateId(2L).subType("merged").build();
        assertEquals("1007553635\tPullRequest\t2010-11-19T04:00:26-08:00\tspecht\tspecht\tproteomatic\t\t\t2\tmerged", e.toString());
    }

    @Test
    public void pullRequestMustHaveRepoAccount() {
        try {
            new Event.Builder(1007553635L, EventType.valueOf("PullRequest"), "2010-11-19T04:00:26-08:00", "specht")
                    .repoName("proteomatic").alternateId(2L).subType("merged").build();
            fail();
        }
        catch (Exception e) {
            assertEquals("PullRequest event must have an associated repository account", e.getMessage());
        }
    }

    @Test
    public void pullRequestMustHaveRepoName() {
        try {
            new Event.Builder(1007553635L, EventType.valueOf("PullRequest"), "2010-11-19T04:00:26-08:00", "specht")
                    .repoAccount("specht").alternateId(2L).subType("merged").build();
            fail();
        }
        catch (Exception e) {
            assertEquals("PullRequest event must have an associated repository name", e.getMessage());
        }
    }

    @Test
    public void pullRequestMustSpecifyId() {
        try {
            new Event.Builder(1007553635L, EventType.valueOf("PullRequest"), "2010-11-19T04:00:26-08:00", "specht")
                    .repoAccount("specht").repoName("proteomatic").subType("merged").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("PullRequest event must specify the id of the pullrequest", e.getMessage());
        }
    }

    @Test
    public void pullRequestMustSpecifyType() {
        try {
            new Event.Builder(1007553635L, EventType.valueOf("PullRequest"), "2010-11-19T04:00:26-08:00", "specht")
                    .repoAccount("specht").repoName("proteomatic").alternateId(2L).build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("PullRequest event must specify the type of the operation", e.getMessage());
        }
    }

    @Test
    public void download() {
        Event e = new Event.Builder(1007541464L, EventType.valueOf("Download"), "2010-11-19T03:55:04-08:00", "Duny")
                .repoAccount("Duny").repoName("foo_my_autoplaylist").subType("uploaded").build();
        assertEquals("1007541464\tDownload\t2010-11-19T03:55:04-08:00\tDuny\tDuny\tfoo_my_autoplaylist\t\t\t\tuploaded", e.toString());
    }

    @Test
    public void downloadMustHaveRepoAccount() {
        try {
            new Event.Builder(1007541464L, EventType.valueOf("Download"), "2010-11-19T03:55:04-08:00", "Duny")
                    .repoName("foo_my_autoplaylist").subType("uploaded").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Download event must have an associated repository account", e.getMessage());
        }
    }

    @Test
    public void downloadMustHaveRepoName() {
        try {
            new Event.Builder(1007541464L, EventType.valueOf("Download"), "2010-11-19T03:55:04-08:00", "Duny")
                    .repoAccount("Duny").subType("uploaded").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Download event must have an associated repository name", e.getMessage());
        }
    }

    @Test
    public void downloadMustSpecifyType() {
        try {
            new Event.Builder(1007541464L, EventType.valueOf("Download"), "2010-11-19T03:55:04-08:00", "Duny")
                    .repoAccount("Duny").repoName("foo_my_autoplaylist").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Download event must specify the type of the operation", e.getMessage());
        }
    }

    @Test
    public void editWiki() {
        Event e = new Event.Builder(1007541615L, EventType.valueOf("Gollum"), "2010-11-19T03:55:27-08:00", "jeroenbaas")
                .repoAccount("Opstandingskerk").repoName("NWonline wiki").subType("edited").build();
        assertEquals("1007541615\tGollum\t2010-11-19T03:55:27-08:00\tjeroenbaas\tOpstandingskerk\tNWonline wiki\t\t\t\tedited", e.toString());
    }

    @Test
    public void updateWiki() {
        Event e = new Event.Builder(1007555427L, EventType.valueOf("Gollum"), "2010-11-19T04:02:27-08:00", "rakd")
                .repoAccount("rakd").repoName("test wiki").subType("created").build();
        assertEquals("1007555427\tGollum\t2010-11-19T04:02:27-08:00\trakd\trakd\ttest wiki\t\t\t\tcreated", e.toString());
    }

    @Test
    public void wikiEventMustSpecifyRepoAccount() {
        try {
            new Event.Builder(1007555427L, EventType.valueOf("Gollum"), "2010-11-19T04:02:27-08:00", "rakd")
                    .repoName("test wiki").subType("created").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Gollum event must have an associated repository account", e.getMessage());
        }
    }

    @Test
    public void wikiEventMustSpecifyRepoName() {
        try {
            new Event.Builder(1007555427L, EventType.valueOf("Gollum"), "2010-11-19T04:02:27-08:00", "rakd")
                    .repoAccount("rakd").subType("created").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Gollum event must have an associated repository name", e.getMessage());
        }
    }

    @Test
    public void wikiEventMustSpecifyType() {
        try {
            new Event.Builder(1007555427L, EventType.valueOf("Gollum"), "2010-11-19T04:02:27-08:00", "rakd")
                    .repoAccount("rakd").repoName("test wiki").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Gollum event must specify the type of the operation", e.getMessage());
        }
    }

    @Test
    public void deleteBranch() {
        Event e = new Event.Builder(1007541502L, EventType.valueOf("Delete"), "2010-11-19T03:55:09-08:00", "clbustos")
                .repoAccount("clbustos").repoName("rubyvis").branch("stack_layout").build();
        assertEquals("1007541502\tDelete\t2010-11-19T03:55:09-08:00\tclbustos\tclbustos\trubyvis\tstack_layout\t\t\t", e.toString());
    }

    @Test
    public void deleteMustSpecifyRepoAccount() {
        try {
            new Event.Builder(1007541502L, EventType.valueOf("Delete"), "2010-11-19T03:55:09-08:00", "clbustos")
                    .repoName("rubyvis").branch("stack_layout").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Delete event must have an associated repository account", e.getMessage());
        }
    }

    @Test
    public void deleteMustSpecifyRepoName() {
        try {
            new Event.Builder(1007541502L, EventType.valueOf("Delete"), "2010-11-19T03:55:09-08:00", "clbustos")
                    .repoAccount("clbustos").branch("stack_layout").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Delete event must have an associated repository name", e.getMessage());
        }
    }

    @Test
    public void deleteTag() {
        Event e = new Event.Builder(1007566134L, EventType.valueOf("Delete"), "2010-11-19T04:21:19-08:00", "thanethomson")
                .repoAccount("thanethomson").repoName("django-form-designer").tag("0.1.praekelt").build();
        assertEquals("1007566134\tDelete\t2010-11-19T04:21:19-08:00\tthanethomson\tthanethomson\tdjango-form-designer\t\t0.1.praekelt\t\t", e.toString());
    }

    @Test
    public void deleteCantSpecifyBranchAndTag() {
        try {
            new Event.Builder(1007541502L, EventType.valueOf("Delete"), "2010-11-19T03:55:09-08:00", "clbustos")
                    .repoAccount("clbustos").repoName("rubyvis").branch("stack_layout").tag("0.1.praekelt").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Can't delete a branch and a tag in the same event", e.getMessage());
        }
    }

    @Test
    public void publishRepo() {
        Event e = new Event.Builder(1007541632L, EventType.valueOf("Public"), "2010-11-19T03:55:33-08:00", "saevarom")
                .repoAccount("saevarom").repoName("playware").build();
        assertEquals("1007541632\tPublic\t2010-11-19T03:55:33-08:00\tsaevarom\tsaevarom\tplayware\t\t\t\t", e.toString());
    }

    @Test
    public void publishMustSpecifyRepoAccount() {
        try {
            new Event.Builder(1007541632L, EventType.valueOf("Public"), "2010-11-19T03:55:33-08:00", "saevarom")
                    .repoName("playware").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Public event must have an associated repository account", e.getMessage());
        }
    }

    @Test
    public void publishMustSpecifyRepoName() {
        try {
            new Event.Builder(1007541632L, EventType.valueOf("Public"), "2010-11-19T03:55:33-08:00", "saevarom")
                    .repoAccount("saevarom").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("Public event must have an associated repository name", e.getMessage());
        }
    }

    @Test
    public void commentOnCommit() {
        Event e = new Event.Builder(1007547055L, EventType.valueOf("CommitComment"), "2010-11-19T03:59:22-08:00", "VladimirMangos")
                .repoAccount("mangos").repoName("mangos").build();
        assertEquals("1007547055\tCommitComment\t2010-11-19T03:59:22-08:00\tVladimirMangos\tmangos\tmangos\t\t\t\t", e.toString());
    }

    @Test
    public void commentMustSpecifyRepoAccount() {
        try {
            new Event.Builder(1007547055L, EventType.valueOf("CommitComment"), "2010-11-19T03:59:22-08:00", "VladimirMangos")
                    .repoName("mangos").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("CommitComment event must have an associated repository account", e.getMessage());
        }
    }

    @Test
    public void commentMustSpecifyRepoName() {
        try {
            new Event.Builder(1007547055L, EventType.valueOf("CommitComment"), "2010-11-19T03:59:22-08:00", "VladimirMangos")
                    .repoAccount("mangos").build();
            fail();
        }
        catch (IllegalArgumentException e) {
            assertEquals("CommitComment event must have an associated repository name", e.getMessage());
        }
    }

}