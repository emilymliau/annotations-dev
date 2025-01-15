from pyspark.sql import SparkSession
import hail as hl
import numpy as np
import pandas as pd
import sys
import os
import datetime

input_vcf = sys.argv[1]
output_vcf = sys.argv[2]
cores = sys.argv[3]
mem = int(np.floor(float(sys.argv[4])))

hl.init(min_block_size=128, spark_conf={"spark.executor.cores": cores, 
                    "spark.executor.memory": f"{int(np.floor(mem*0.4))}g",
                    "spark.driver.cores": cores,
                    "spark.driver.memory": f"{int(np.floor(mem*0.4))}g"
                    }, tmp_dir="tmp", local_tmpdir="tmp")

mt = hl.import_vcf(input_vcf)

    # calculate FORMAT-level DP by summing the AD fields per sample
    mt = mt.annotate_entries(DP=hl.sum(mt.AD))

    # calculate INFO-level DP by summing across samples
    mt = mt.annotate_rows(INFO_DP=hl.agg.sum(mt.DP))

    hl.export_vcf(mt, output_vcf)