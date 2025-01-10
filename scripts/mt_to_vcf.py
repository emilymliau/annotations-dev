from pyspark.sql import SparkSession
import hail as hl
import numpy as np
import pandas as pd
import sys
import os
import datetime

mt_uri = sys.argv[1]
bucket_id = sys.argv[2]
cores = sys.argv[3]  # string
mem = int(np.floor(float(sys.argv[4])))

prefix = os.path.basename(mt_uri).split('.mt')[0]

hl.init(min_block_size=128, spark_conf={"spark.executor.cores": cores, 
                    "spark.executor.memory": f"{int(np.floor(mem*0.4))}g",
                    "spark.driver.cores": cores,
                    "spark.driver.memory": f"{int(np.floor(mem*0.4))}g"
                    }, tmp_dir="tmp", local_tmpdir="tmp")

mt = hl.read_matrix_table(mt_uri)
vcf_filename = f"{bucket_id}/vep-annotate-hail-mt/{str(datetime.datetime.now().strftime('%Y-%m-%d_%H-%M'))}/{prefix}.vcf.gz"
hl.export_vcf(mt, vcf_filename)
pd.Series([vcf_filename]).to_csv('vcf_uri.txt', index=False, header=None)