# How to release Graft

Starting from a clean local repo state on the main branch.

First figure out the version you want to release. The latest version can be found using: `git describe --tags --abbrev=0` and you might also find `cargo release changes` helpful.

When you are ready to release, run:

```
just run release --execute <VERSION NUMBER>
```

This script will prepare the release and push it to a release branch named `release/<VERSION NUMBER>`.

Next, create a PR for the release branch and wait until the `release-prep.yml` workflow has finished. Address any issues it hits in the release PR.

`release-prep` will create a GitHub Release in draft mode with draft notes. Once the workflow has succeeded with no errors and you are ready to release, update the draft GitHub Release notes to ensure they clearly outline what has actually changed.

`release-prep` will also trigger the `release.yml` workflow on the main branch. This workflow will wait for manual approval and will replace itself as needed. Do not approve this workflow until after the PR has landed.

Finally, it's time to release! Merge the PR and approve the most recent `release.yml` workflow. This will cause all of the actual releases to go out to the various package managers as well as pushing a git tag and publishing the GitHub release.

Success! Hopefully. Go and check everything. But assuming it all looks good, great job!
