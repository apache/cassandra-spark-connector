# Description

## How did the Spark Cassandra Connector Work or Not Work Before this Patch

Describe the problem, or state of the project that this patch fixes. Explain
why this is a problem if this isn't obvious.

Example: 
  "When I read from tables with 3 INTS I get a ThreeIntException(). This is a problem because I often want to read from a table with three integers."

## General Design of the patch

How the fix is accomplished, were new parameters or classes added? Why did you
pursue this particular fix?

Example: "I removed the incorrect assertion which would throw the ThreeIntException. This exception was incorrectly added and the assertion is not actually needed."

Fixes: [Put JIRA Reference HERE](https://issues.apache.org/jira/projects/CASSANALYTICS)

# How Has This Been Tested?

Almost all changes and especially bug fixes will require a test to be added to either the integration or Unit Tests. Any tests added will be automatically run on travis when the pull request is pushed to github. Be sure to run suites locally as well.

# Checklist:

- [ ] I have a ticket in the [JIRA](https://issues.apache.org/jira/projects/CASSANALYTICS)
- [ ] I have performed a self-review of my own code
- [ ] Locally all tests pass (make sure tests fail without your patch)
