# Hubstats

## About
Hadoop tool to import data from the GitHub public timeline XML format.

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
