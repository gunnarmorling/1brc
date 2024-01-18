#!/bin/bash

: "${REVIEW_BASE_BRANCH:=main}"

git merge --abort
git checkout "${REVIEW_BASE_BRANCH}"

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

review_branch="review/${author}"
if git branch -v | egrep -s "^\s+${review_branch}\s+"; then
  git branch -d "${review_branch}"
fi

remote="review-${author}"
if git remote | grep -s "${remote}"; then
  git remote remove "${remote}"
fi

./reset-sdk.sh

./mvnw clean verify -Dlicense.skip
