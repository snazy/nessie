# Running Nessie workflows with https://github.com/nektos/act

**THE CONTENTS IN THIS DIRECTORY ARE PURELY TO TEST GITHUB WORKFLOWS LOCALLY!**

## Build a Docker image w/ `gh`

```
cd image
docker build --force-rm --rm --tag projectnessie/act-environments-gh-ubuntu:18.04 --file Dockerfile .
```

## git remote name

Make sure that the name of the git remote is `origin`

## Install nektos/act

1. Install https://github.com/nektos/act
2. Update `~/.actrc`
    Create a personal access token and add it:
      `--secret GITHUB_TOKEN=ghp_blahblah`
    Change:
      `-P ubuntu-latest=projectnessie/act-environments-gh-ubuntu:18.04`


## Running the "Release Create" workflow:

1. Check/update `release-create.json`
2. Run `act -v workflow_dispatch -j create-release -e .github/act-test/release-create.json`
3. The "Release Publish" workflow will start automatically on GitHub, but it's tricky to get
   it triggered (actually: only triggered for the new tag) with act.

## Running the "Release Publish" workflow for a `push`

1. Check/update `release-publish.json`
2. Run `act -v push -j publish-release -e .github/act-test/release-publish.json`

## Running the "Release Publish" workflow for a `workflow_dispatch`

1. Check/update `release-publish.json`
2. Run `act -v workflow_dispatch -j publish-release -e .github/act-test/release-publish-manual.json`

## Notes

Be careful when just running `act` - it will start the matching workflows, for example the whole
"Main CI" spiel.

