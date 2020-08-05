# Butterfree's WorkFlow

## Features

A feature is based on the `master` branch and merged back into the `master` branch.

![](https://docs.microsoft.com/en-us/azure/devops/repos/git/media/branching-guidance/featurebranching.png?view=azure-devops)


### Working Locally

```
# checkout master, fetch the latest changes and pull them from remote into local
git checkout master
git fetch
git pull origin master

# create a feature branch that is based off master
git checkout -b <username>/some-description

# do your work
git add something
git commit -m "first commit"
git add another
git commit -m "second commit"

# rebase against master to pull in any changes that have been made
# since you started your feature branch.
git fetch
git rebase origin/master

# push your local changes up to the remote
git push

# if you've already pushed changes and have rebased, your history has changed
# so you will need to force the push
git fetch
git rebase origin/master
git push --force-with-lease
````


### GitHub workflow

- Open a Pull Request against `master`. Check our PR guidelines [here](https://github.com/quintoandar/butterfree/blob/master/CONTRIBUTING.md#pull-request-guideline).
- When the Pull Request has been approved, merge using `squash and merge`, adding a brief description:
ie, ` Enable stream pipelines in Butterfree`.
- This squashes all your commits into a single clean commit. Remember to clean detailed descriptions, otherwise our git logs will be a mess.

If you are unable to squash merge because of conflicts, you need to rebase against `master` again:

```
# in your feature branch
git fetch
git rebase origin/master
# fix conflicts if they exist
git push --force-with-lease
```


## Releases

The release will always occur when we change the version in the setup.py file.


### Working Locally

```
# just change the version on 
git checkout master
git fetch
git pull origin master

# finalize the changelog, bump the version into setup.py and update the documentation then:
make update-docs
git add .
git commit -m "release <version>"

# push the new version
git fetch
git push --force-with-lease
```

If there are any issues, fixes should be committed (or merged in) to the release branch.

### Github workflow

- A tag will automatically be triggered in our CI/CD. This tag/release will use the version for its title and push a new version
of Butterfree's python package to our private server.

### Update API Documentation

If new information was added in the documentation in the release, maybe you will need to update our hosted Documentation. It's super simple, in the **docs** folder just apply the modifications and open a PR:

If you want to test your changes locally, just run:
 
```bash
make docs
```

And open `index.html` file. 

No need to worry about modifying the `API Documentation`,  everything is generated from [Sphinx](https://www.sphinx-doc.org/en/master/index.html) and hosted by [ReadtheDocs](https://readthedocs.org/). But your documentation changes will only be applied after a merge to master branch.


## Hotfixes

A hotfix is a patch that needs to go directly into `master` without going through the regular release process.
The most common use case is to patch a bug that's on production when `hotfix` contains code that isn't yet ready for release.

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

Another use case is when a past release needs a patch. For example, we are currently on version 3.2 but find a critical 
bug that is present since 2.5 and want to fix it. Then we would create a hotfix branch and release it as 2.5.1.

### Working locally

```
# create a hotfix branch based on master, because master is what will be deployed to production
git checkout master@2.5
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
- A tag will automatically be triggered in our CI/CD. This tag/release will use the version for its title and push a new version
of Butterfree's python package to our private server.

You may always update the tag/release description with the changelog.
