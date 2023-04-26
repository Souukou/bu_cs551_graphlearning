JOB_ID=$(flink run --detached -pyfs src/main/python target/GraphOperators.jar  --properties prop.config --pyscript src/main/python/train.py --model-path /tmp/linear/model | awk '/JobID/ {print $NF}')

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

python "metrics/RetrieveThroughput.py" "$JOB_ID" &
python "metrics/RetrieveLatency.py" "$JOB_ID" &
wait

