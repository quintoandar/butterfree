# Butterfree's Git Flow
Based on [markreid's notes](https://gist.github.com/markreid/12e7c2203916b93d23c27a263f6091a0).

## Features

A feature is based on the `staging` branch and merged back into the `staging` branch.
It will eventually get into `master` when we make a release.


### Working Locally

```
# checkout develop, fetch the latest changes and pull them from remote into local
git checkout staging
git fetch
git pull origin staging

# create a feature branch that is based off develop
git checkout -b <username>/some-description

# do your work
git add something
git commit -m "first commit"
git add another
git commit -m "second commit"

# rebase against staging to pull in any changes that have been made
# since you started your feature branch.
git fetch
git rebase origin/develop

# push your local changes up to the remote
git push

# if you've already pushed changes and have rebased, your history has changed
# so you will need to force the push
git fetch
git rebase origin/develop
git push --force-with-lease
````


### GitHub workflow

- Open a Pull Request against `staging`. Check our PR guidelines [here](https://github.com/quintoandar/butterfree/blob/master/CONTRIBUTING.md#pull-request-guideline).
- When the Pull Request has been approved, merge using `squash and merge`, adding the Jira task number and a brief description:
ie, `[MLOP-169] Enable stream pipelines in Butterfree`.
- This squashes all your commits into a single clean commit. Remember to clean detailed descriptions, otherwise our git logs will be a mess.

If you are unable to squash merge because of conflicts, you need to rebase against `staging` again:

```
# in your feature branch
git fetch
git rebase origin/staging
# fix conflicts if they exist
git push --force-with-lease
```


## Releases

A release takes the changes in `staging` and applies them to `master`.


### Working locally


```
# create a release branch from staging
git checkout staging
git fetch
git pull origin staging
git checkout -b release/0.4.0

# finalize the changelog, dump the version into setup.py, then:
git add CHANGELOG.md
git commit -m "release 0.4.0"

# rebase against master, which we're going to merge into
git fetch
git rebase origin/master
git push --force-with-lease
```

If there are any issues, fixes should be committed (or merged in) to the release branch.

### Github workflow

- Open a Pull Request against `master`
- When the PR is approved and the staging deploy has been verified by QA, merge using `rebase and merge`.
- **DO NOT SQUASH MERGE**. We don't want a single commit for the release, we want to maintain the feature commits in the history.
- Repeat the steps above against `staging` (may need to rebase first).
- A tag will automatically be triggered in our CI/CD. This tag/release will use the version for its title and push a new version
of Butterfree's python package to our private server.

### Update API Documentation

If new information was added in the documentation in the release. You will need to update our hosted API Documentation. It's super simple, in the **documentation** branch  just bring the modifications from the master branch and run:

```bash
make update-docs
```

No need to worry about modifying the `html files`,  everything is generated from [Sphinx](https://www.sphinx-doc.org/en/master/index.html) and hosted by [ReadtheDocs](https://readthedocs.org/). But your documentation changes will only be applied after a merge to documentation branch.


## Hotfixes

A hotfix is a patch that needs to go directly into `master` without going through the regular release process.
The most common use case is to patch a bug that's on production when `staging` contains code that isn't yet ready for release.


### Working locally

```
# create a hotfix branch based on master, because master is what will be deployed to production
git checkout master
git fetch
git pull origin master
git checkout -b hotfix/describe-the-problem

git add patch.fix
git commit -m "fix the problem"
git push
```

Don't forget to update the Changelog and the version in `setup.py`.

### Github workflow

- Open a Pull Request against `master`
- When the PR's approved and the code is tested, `squash and merge` to squash your commits into a single commit.
- Open a Pull Request against `staging` (may need to rebase first).
- A tag will automatically be triggered in our CI/CD. This tag/release will use the version for its title and push a new version
of Butterfree's python package to our private server.

You may always update the tag/release description with the changelog.
