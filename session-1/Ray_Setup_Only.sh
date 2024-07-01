#RAY PORT
RAY_PORT=9339
REDIS_PASS="d4t4bricks"

# install ray
/databricks/python/bin/pip install ray==2.9.0

# Install additional ray libraries
# /databricks/python/bin/pip install ray[debug,dashboard,tune,rllib,serve]
# If starting on the Spark driver node, initialize the Ray head node
# If starting on the Spark worker node, connect to the head Ray node
if [ ! -z $DB_IS_DRIVER ] && [ $DB_IS_DRIVER = TRUE ] ; then
  echo "Starting the head node"
  ray start  --min-worker-port=20000 --max-worker-port=25000 --temp-dir="/tmp/ray" --head --port=$RAY_PORT --redis-password="$REDIS_PASS"  --include-dashboard=false
else
  sleep 40
  echo "Starting the non-head node - connecting to $DB_DRIVER_IP:$RAY_PORT"
  ray start  --min-worker-port=20000 --max-worker-port=25000 --temp-dir="/tmp/ray" --address="$DB_DRIVER_IP:$RAY_PORT" --redis-password="$REDIS_PASS"
fi