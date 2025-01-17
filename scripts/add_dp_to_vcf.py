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
build = sys.argv[5]

hl.init(min_block_size=128, spark_conf={"spark.executor.cores": cores, 
                    "spark.executor.memory": f"{int(np.floor(mem*0.4))}g",
                    "spark.driver.cores": cores,
                    "spark.driver.memory": f"{int(np.floor(mem*0.4))}g"
                    }, tmp_dir="tmp", local_tmpdir="tmp")

print(f"Processing started at: {datetime.datetime.now()}")
header = hl.get_vcf_metadata(input_vcf)
mt = hl.import_vcf(input_vcf, reference_genome=build)

header['info']['DP'] = {'Description': 'Approximate read depth (estimated as sum of AD).', 'Number': '1', 'Type': 'Integer'}
header['format']['DP'] = {'Description': 'Approximate read depth (estimated as sum of AD).', 'Number': '1', 'Type': 'Integer'}

# calculate FORMAT-level DP (sum AD fields per sample)
print(f"Calculating FORMAT-level DP...")
mt = mt.annotate_entries(DP=hl.sum(mt.AD))

# calculate INFO-level DP (sum of FORMAT-level DP)
print(f"Calculating INFO-level DP...")
mt = mt.annotate_rows(INFO_DP=hl.agg.sum(mt.DP))

hl.export_vcf(mt, output_vcf, metadata=header, tabix=True)
print(f"Processing finished at: {datetime.datetime.now()}")
print("Output VCF: ", output_vcf)