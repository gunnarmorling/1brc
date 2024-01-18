#!/bin/bash

set -euo pipefail

if ! type -p gh >/dev/null; then
  echo "Please install the 'gh' tool, e.g., via Homebrew: brew install gh" >&2
  exit 1
fi

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <pr>" >&2
  exit 1
fi

pr="${1}"

author="$(gh pr view "${pr}" --json author -q .author.login)"
repository="$(gh pr view "${pr}" --json headRepository -q .headRepository.name)"
branch="$(gh pr view "${pr}" --json baseRefName -q .baseRefName)"

remote="review-${author}"
if ! git remote | grep -s "${remote}"; then
  git remote add "${remote}" "git@github.com:${author}/${repository}"
fi

current_branch="$(git rev-parse --abbrev-ref HEAD)"
new_branch="review/${author}"
if [ "${current_branch}" != "${new_branch}" ]; then
  git checkout -b "${new_branch}"
fi

git fetch "${remote}"
#gh pr merge "${pr}"

git merge --no-commit "${remote}/${branch}"

./mvnw clean verify -Dlicense.skip

./test.sh "${author}"
test -x test-unique.sh && ./test-unique.sh "${author}"

gh pr view "${pr}" -c

if test "${EDITOR:-}"; then
  files="$(gh pr view "${pr}" --json files -q .files[].path)"
  # shellcheck disable=SC2086
  "${EDITOR}" ${files}
fi