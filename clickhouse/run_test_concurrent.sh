#!/bin/bash

# Number of concurrent queries
CONCURRENT_JOBS=5

# Determine which set of files to use depending on the type of run
if [[ "$1" != "" && "$1" != "tuned" && "$1" != "tuned-memory" ]]; then
    echo "Error: command line argument must be one of {'', 'tuned', 'tuned-memory'}"
    exit 1
elif [[ ! -z "$1" ]]; then
    SUFFIX="-$1"
fi

TRIES=3
QUERY_NUM=1

# For capturing output cleanly from multiple background jobs,
# use a lock file for result file writing
LOCKFILE="./result.csv.lock"
RESULTFILE="result.csv"

touch "$RESULTFILE"

run_query() {
    local query="$1"
    local qnum="$2"
    local tries="$3"
    local result_output="["

    [ -z "$FQDN" ] && sync
    [ -z "$FQDN" ] && echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null

    for i in $(seq 1 "$tries"); do
        RES=$(clickhouse-client --host=localhost --user=test --password=test --time --format=Null --query="$query" --progress 0 2>&1 || :)
        [[ "$?" == "0" ]] && result_output+="${RES}" || result_output+="null"
        [[ "$i" != $tries ]] && result_output+=", "

        # Use lock for consistent multi-process writing
        { flock 9; echo "${qnum},${i},${RES}" >> "$RESULTFILE"; } 9>>"$LOCKFILE"
    done

    result_output+="],"
    echo -n "$result_output"
}

export -f run_query
export RESULTFILE LOCKFILE

cat queries"$SUFFIX".sql | while read -r query; do
    # Skip empty lines
    [[ -z "$query" ]] && continue

    run_query "$query" "$QUERY_NUM" "$TRIES" &

    # Control concurrency
    while [[ $(jobs -r | wc -l) -ge $CONCURRENT_JOBS ]]; do
        sleep 0.1
    done

    QUERY_NUM=$((QUERY_NUM + 1))
done

# Wait for remaining jobs to finish
wait
