package hubstats;

/**
 * A GitHub event. Use Event.Builder to construct a new instance.
 * @see Event.Builder
 */
public class Event {

    private static final char SEP = '\t';
    
    private final long eventId;
    private final EventType eventType;
    private final String at;
    private final String actor;
    private String repoAccount;
    private String repoName;
    private String branch;
    private final String tag;
    private final long alternateId;
    private final String subType;

    /**
     * Builder to ease the creation of new instances of Event.
     */
    public static class Builder {
        // Required parameters
        private final long eventId;
        private final EventType eventType;
        private final String at;
        private final String actor;

        // Optional parameters
        private String repoAccount;
        private String repoName;
        private String branch;
        private String tag;
        private long alternateId;
        private String subtype;

        public Builder(long eventId, EventType eventType, String at, String actor) {
            this.eventId = eventId;
            this.eventType = eventType;
            this.at = at;
            this.actor = actor;
        }

        public Builder repoAccount(String account) {
            this.repoAccount = account;
            return this;
        }

        public Builder repoName(String name) {
            this.repoName = name;
            return this;
        }

        public Builder branch(String branch) {
            this.branch = branch;
            return this;
        }

        public Builder tag(String tag) {
            this.tag = tag;
            return this;
        }

        public Builder alternateId(long id) {
            this.alternateId = id;
            return this;
        }

        public Builder subType(String type) {
            this.subtype = type;
            return this;
        }

        /**
         * Build a new Event based on the values provided to this builder. Validation occurs at this point that the
         * event values provided are valid.
         * @return A new Event
         */
        public Event build() {
            return new Event(this);
        }

    }

    private Event(Builder builder) {

        if (builder.eventId <= 0) {
            throw new IllegalArgumentException(String.format("Event id must be greater than zero (was %s)",
                    builder.eventId));
        }

        if (builder.actor == null || builder.actor.trim().equals("")) {
            throw new IllegalArgumentException("Actor account name cannot be null or empty");
        }
        else {
            this.actor = builder.actor.trim();
        }

        if (! builder.eventType.needsAccount()) {
            if (builder.repoAccount != null) {
                throw new IllegalArgumentException(
                        String.format("%s event must not have an associated repository account", builder.eventType));
            }
        }
        else {
            if (builder.repoAccount == null || builder.repoAccount.trim().equals("")) {
                throw new IllegalArgumentException(String.format("%s event must have an associated repository account", builder.eventType));
            }
        }

        if (! builder.eventType.needsRepoName()) {
            if (builder.repoName != null) {
                throw new IllegalArgumentException(String.format("%s event must not have an associated repository name",
                        builder.eventType));
            }
        }
        else {
            if (builder.repoName == null || builder.repoName.trim().equals("")) {
                throw new IllegalArgumentException(String.format("%s event must have an associated repository name", builder.eventType));
            }
        }

        if (builder.eventType == EventType.Push) {
            if (builder.branch == null || builder.branch.trim().equals("")) {
                throw new IllegalArgumentException("Push event must have an associated branch");
            }
        }
        else if (builder.eventType == EventType.Create || builder.eventType == EventType.Delete) {
            if (builder.branch != null && builder.tag != null) {
                throw new IllegalArgumentException(String.format("Can't %s a branch and a tag in the same event",
                        builder.eventType.toString().toLowerCase()));    
            }
        }

        if (builder.alternateId == 0L && builder.eventType.requiresId()) {
                throwMustSpecify(builder.eventType, "id");
        }
        else if (builder.subtype == null && builder.eventType.requiresType()) {
            throwMustSpecify(builder.eventType, "type");
        }

        this.eventId = builder.eventId;
        this.eventType = builder.eventType;
        this.at = builder.at;
        if (builder.repoAccount != null) {
            this.repoAccount = builder.repoAccount.trim();
        }
        if (builder.repoName != null) {
            this.repoName = builder.repoName.trim();
        }
        if (builder.branch != null) {
            this.branch = builder.branch.trim();
        }
        this.tag = builder.tag;
        this.alternateId = builder.alternateId;
        this.subType = builder.subtype;
    }

    private void throwMustSpecify(EventType type, String field) {
        String str = "operation";
        if (field.equals("id")) {
            str = type.toString().toLowerCase().replaceFirst("s$", "");
        }
        throw new IllegalArgumentException(String.format("%s event must specify the %s of the %s", type, field, str));
    }

    String getActor() {
        return this.actor;
    }

    String getRepoAccount() {
        return this.repoAccount;
    }

    String getRepoName() {
        return this.repoName;
    }

    String getBranch() {
        return this.branch;
    }

    /**
     * Returns the string representation of this Event. The format is a single line, separated by tabs. The format of
     * each line may not be the same between versions of this class.
     *
     * The fields are:
     * <ol>
     * <li>event_id - The unique numeric event id</li>
     * <li>event_type - The type of event</li>
     * <li>at - The date and time of the event</li>
     * <li>actor - The GitHub account that created the event</li>
     * <li>repo_account - The GitHub account associated with the related repository</li>
     * <li>repo_name - The name of the repository</li>
     * <li>branch - The Git branch name</li>
     * <li>tag - The Git tag name</li>
     * <li>alternate_id - An alternative numeric id if the event relates to an Issue, PullRequest or Gist</li>
     * <li>subtype - Further subtype of the event, for example an Issues event can be 'Opened' or 'Closed'</li>
     * </ol>
     *
     * @return This event as a String.
     **/
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append(eventId);
        buf.append(SEP);
        buf.append(eventType);
        buf.append(SEP);
        buf.append(at);
        buf.append(SEP);
        buf.append(actor);
        buf.append(SEP);
        if (repoAccount != null) {
            buf.append(repoAccount);
        }
        buf.append(SEP);
        if (repoName != null) {
            buf.append(repoName);
        }
        buf.append(SEP);
        if (branch != null) {
            buf.append(branch);
        }
        buf.append(SEP);
        if (tag != null) {
            buf.append(tag);
        }
        buf.append(SEP);
        if (alternateId != 0L) {
            buf.append(alternateId);
        }
        buf.append(SEP);
        if (subType != null) {
            buf.append(subType);
        }
        return buf.toString();
    }
}
