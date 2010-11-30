# Hubstats

## About
Hadoop tool to import data from the GitHub public timeline XML format.

Run with:

    $ hadoop jar hubstats.jar hubstats.HubStats input output

## Output format
The following fields are output:

 *  *event_id* - The unique numeric event id
 *  *event_type* - The type of event
 *  *at* - The date and time of the event
 *  *actor* - The GitHub account that created the event
 *  *repo_account* - The GitHub account associated with the related repository
 *  *repo_name* - The name of the repository
 *  *branch* - The Git branch name
 *  *tag* - The Git tag name
 *  *alternate_id* - An alternative numeric id if the event relates to an Issue, PullRequest or Gist
 *  *subtype* - Further subtype of the event, for example an Issues event can be 'Opened' or 'Closed'

## Sample Output
Sample output from a run over 30 gigs of the timeline requests (about 300,000 GitHub events in a bit over 11 days) is available in `sample-output.gz`.

    $ gzcat sample-output.gz | awk '{print $3}' | sort | uniq -c | sort -nr
    173260 Push
    48186 Create
    34782 Watch
    12256 Gist
    11756 Issues
    9453 Follow
    8731 Fork
    8202 Gollum
    6672 PullRequest
    5132 Delete
    4027 CommitComment
    2527 Member
    1666 Download
    1053 ForkApply
     288 Public