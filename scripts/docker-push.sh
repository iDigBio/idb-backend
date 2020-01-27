# need to replace a forward slash for handling git flow feature/my-feature-name branches
export TRAVIS_BRANCH_REPLACED=$(printf "%s" "$TRAVIS_BRANCH" | sed 's#/#-#g')

docker tag $BUILDING $DOCKER_IMAGE:$TRAVIS_BRANCH_REPLACED
docker push $DOCKER_IMAGE:$TRAVIS_BRANCH_REPLACED