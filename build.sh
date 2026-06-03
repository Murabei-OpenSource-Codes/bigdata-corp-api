source VERSION
sed -e 's#{VERSION}#'"${VERSION}"'#g' pyproject_template.toml > pyproject.toml

poetry build

git add --all
git commit -m "Building a new version ${VERSION}"
git tag -a ${VERSION} -m "Building a new version ${VERSION}"
git push
git push origin ${VERSION}