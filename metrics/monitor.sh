JOB_ID=$($FLINK_PATH/bin/flink run --detached target/GraphOperators.jar  | awk '/JobID/ {print $NF}')

if [ -z "$JOB_ID" ]; then
    echo "Failed to extract job ID."
    exit 1
fi

directory_path="results"

if [ ! -d "$directory_path" ]; then
    mkdir "$directory_path"
    echo "Directory $directory_path created."
else
    echo "Directory $directory_path already exists."
fi

python "RetrieveThroughput.py" "$JOB_ID" &
python "RetrieveLatency.py" "$JOB_ID" &
wait

