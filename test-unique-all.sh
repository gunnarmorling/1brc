#!/bin/bash

: "${UNIQUE=100000}"
users="$(awk -F\; '{print $1}' github_users.txt | sort -u)"

mkdir -p logs_prefix

logs_prefix="logs/test-unique-${UNIQUE}"
for user in ${users}; do
  if ./test-unique.sh "${user}" > "${logs_prefix}-${user}.log" 2> "${logs_prefix}-${user}.err"; then
    echo "Worked for user '${user}'"
  else
    echo "Failed for user '${user}'"
  fi
done