#!/usr/bin/env bash

make version
make package-name

VERSION=$(cat ./.version)
PACKAGE_NAME=$(cat ./.package_name)
if [[ -d "python-package-server/${PACKAGE_NAME}" ]]; then
    if grep -q "${PACKAGE_NAME}-${VERSION}" ./python-package-server/${PACKAGE_NAME}/index.html; then
        echo ""
        echo "ERROR: Version ${VERSION} already exists for ${PACKAGE_NAME}"
        echo "Upgrade this project '__version__' variable in 'setup.py'"
        echo "Refer to http://semver.org/ for versioning standards"
        echo ""
        exit 1
    fi
fi
echo ""
echo "SUCCESS: There are no conflicts for publishing version ${VERSION} for ${PACKAGE_NAME}"
echo ""
exit 0
