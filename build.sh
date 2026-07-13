# Update version path
VERSION_FILE="VERSION"
current_version=$(grep -E "^VERSION=" "$VERSION_FILE" | cut -d'=' -f2)
IFS='.' read -r major minor patch <<< "$current_version"
patch=$((patch + 1))
new_version="$major.$minor.$patch"
sed -i "s/^VERSION=.*/VERSION=$new_version/" "$VERSION_FILE"
echo "Updated version to $new_version"

source VERSION

# Detecta branch atual e adiciona -beta se não for main/master
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [[ "$CURRENT_BRANCH" != "main" && "$CURRENT_BRANCH" != "master" ]]; then
  VERSION="${VERSION}-b.0"
fi

sed -e 's#{VERSION}#'"${VERSION}"'#g' pyproject_template.toml > pyproject.toml

rm -R build/

poetry build

git add --all
git commit -m "Building a new version ${VERSION}"
git tag -a ${VERSION} -m "Building a new version ${VERSION}"
git push
git push origin ${VERSION}
