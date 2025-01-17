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

print(f"...VCF processing started at: {datetime.datetime.now()}")
header = hl.get_vcf_metadata(input_vcf)
mt = hl.import_vcf(input_vcf, force_bgz=True, array_elements_required=False, call_fields=[], reference_genome=build)

# drop existing DP fields for testing
if 'DP' in mt.entry:
    mt = mt.drop(mt.DP)
if 'DP' in mt.row:
    mt = mt.drop(mt.info.DP)

# calculate FORMAT-level DP (sum of AD fields per sample)
print(f"...calculating FORMAT-level DP")
mt = mt.annotate_entries(DP=hl.sum(mt.AD))
header['format']['DP'] = {'Description': 'Approximate read depth, estimated as sum of AD per sample.', 'Number': '1', 'Type': 'Integer'}

# calculate INFO-level DP (sum of AD fields across samples)
print(f"...calculating INFO-level DP")
# mt = mt.annotate_rows(info=mt.info.annotate(DP=hl.agg.sum(mt.DP)))
mt = mt.annotate_rows(info=mt.info.annotate(DP=hl.agg.sum(hl.sum(mt.AD))))
header['info']['DP'] = {'Description': 'Approximate read depth, estimated as sum of AD across samples.', 'Number': '1', 'Type': 'Integer'}
print(f"...DP calculations completed at: {datetime.datetime.now()}")

hl.export_vcf(mt, output_vcf, metadata=header, tabix=True)
print(f"...VCF export completed at: {datetime.datetime.now()}")
print("Output VCF: ", output_vcf)