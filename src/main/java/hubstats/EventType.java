package hubstats;

/**
 * The recognised GitHub events.
 */
public enum EventType {

    /**
     * Push of commits to the repository
     */
    Push(),

    /**
     * Creation of a new repository, branch or tag
     */
    Create(),

    /**
     * A user has started watching a repository
     */
    Watch(),

    /**
     * Issues are opened or closed
     */
    Issues(true, true, true, true),

    /**
     * A user has started following another account
     */
    Follow(true, false, false, false),

    /**
     * A repository has been forked
     */
    Fork(),

    /**
     * A Gist has been created, updated or forked
     */
    Gist(false, false, true, true),

    /**
     * A member has been added to a repository
     */
    Member(true, true, false, true),

    /**
     * A Pull Request to merge changes from another repository has been opened, merged or closed
     */
    PullRequest(true, true, true, true),

    /**
     * A file has been uploaded
     */
    Download(true, true, false, true),

    /**
     * A wiki page has been created or edited
     */
    Gollum(true, true, false, true),

    /**
     * A repository branch or tag has been deleted
     */
    Delete(),

    /**
     * A repository has been made publically available
     */
    Public(),

    /**
     * A comment has been associated with a repository commit
     */
    CommitComment();

    private boolean needsAccount;
    private boolean needsRepoName;
    private boolean requiresId;
    private boolean requiresType;

    private EventType() {
        this(true, true, false, false);
    }

    private EventType(boolean needsAccount, boolean needsRepoName, boolean requiresId, boolean requiresType) {
        this.needsAccount = needsAccount;
        this.needsRepoName = needsRepoName;
        this.requiresId = requiresId;
        this.requiresType = requiresType;
    }

    /**
     * If this event type needs an account to be specified
     * @return True if an account is needed, false means that an account must not be specified
     */
    boolean needsAccount() {
        return this.needsAccount;
    }

    /**
     * If this event type needs an repository name to be specified
     * @return True if a repository name is needed, false means that a repository name must not be specified
     */
    boolean needsRepoName() {
        return this.needsRepoName;
    }

    /**
     * If this event type needs an additional id
     * @return True if an id is required, false means that an id may or may not be specified
     */
    boolean requiresId() {
        return this.requiresId;
    }

    /**
     * If this event type needs an additional type
     * @return True if an type is required, false means that a type may or may not be specified
     */
    boolean requiresType() {
        return this.requiresType;
    }

}
