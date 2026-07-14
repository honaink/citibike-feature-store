from citibike_fs.logs import setup_logging
from citibike_fs.spark.batch_features import main

setup_logging()
main()
