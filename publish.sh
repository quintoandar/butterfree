#!/usr/bin/env bash

make version
if [ -z "$1" ]
  then
    make package-name
else
  make package-name build=-"$1"
  make commit-hash
fi
make repository-url

if [[ ! -d "python-package-server" ]]; then
    git clone https://github.com/quintoandar/python-package-server.git;
fi

cd python-package-server

if [[ ! -d "$(cat ../.package_name)" ]]; then
    mkdir "$(cat ../.package_name)"
fi

if [[ ! -f "$(cat ../.package_name)/index.html" ]]; then
    cat > "$(cat ../.package_name)"/index.html << EOF
<!DOCTYPE html>
<html>
<head>
    <title>Links for $(cat ../.package_name)</title>
</head>
<body>
<h1>Links for $(cat ../.package_name)</h1>
<!-- package-server-links-start -->
</body>
</html>
EOF
fi

if [ -z "$1" ]
  then
    sed -i '/<!-- package-server-links-start -->/ a <a href="git+'"$(cat ../.repository_url)"'@'"$(cat ../.version)"'#egg='"$(cat ../.package_name)"'-'"$(cat ../.version)"'">'"$(cat ../.package_name)"'-'"$(cat ../.version)"'</a><br/>' ./"$(cat ../.package_name)"/index.html
else
  sed -i '/<!-- package-server-links-start -->/ a <a href="git+'"$(cat ../.repository_url)"'@'"$(cat ../.commit_hash)"'#egg='"$(cat ../.package_name)"'-'"$(cat ../.version)"'.'"$(head -c 7 ../.commit_hash)"'">'"$(cat ../.package_name)"'-'"$(cat ../.version)"'.'"$(head -c 7 ../.commit_hash)"'</a><br/>' ./"$(cat ../.package_name)"/index.html
fi

git config user.email "${DRONE_COMMIT_AUTHOR_EMAIL}"
git config user.name "${DRONE_COMMIT_AUTHOR}"
git add .
if [ -z "$1" ]
  then
    git commit -m "$(cat ../.package_name) version $(cat ../.version)"
else
  git commit -m "$(cat ../.package_name) version $(cat ../.version).$(head -c 7 ../.commit_hash)"
fi
git push https://${GITHUB_TOKEN}@github.com/quintoandar/python-package-server master
