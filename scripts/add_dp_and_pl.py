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

# for haploid calls in VCF (e.g. chrY)
mt = mt.annotate_entries(
    GT = hl.if_else(
            mt.GT.ploidy == 1, 
            hl.call(mt.GT[0], mt.GT[0]),
            mt.GT)
)

# drop existing DP & PL fields for testing
if 'DP' in mt.entry:
    mt = mt.drop(mt.DP) # FORMAT-level DP
if 'DP' in mt.row:
    mt = mt.drop(mt.info.DP) # INFO-level DP
if 'PL' in mt.entry:
    mt = mt.drop(mt.PL) # FORMAT-level PL

# calculate FORMAT-level DP (sum of AD fields per sample)
print(f"...calculating FORMAT-level DP")
mt = mt.annotate_entries(DP=hl.sum(mt.AD))
header['format']['DP'] = {'Description': 'Approximate read depth, estimated as sum of AD per sample.', 'Number': '1', 'Type': 'Integer'}
print(f"...FORMAT-level DP calculations completed at: {datetime.datetime.now()}")

# calculate INFO-level DP (sum of AD fields across samples)
print(f"...calculating INFO-level DP")
mt = mt.annotate_rows(info=mt.info.annotate(DP=hl.agg.sum(hl.sum(mt.AD))))

header['info']['DP'] = {'Description': 'Approximate read depth, estimated as sum of AD across samples.', 'Number': '1', 'Type': 'Integer'}
print(f"...INFO-level DP calculations completed at: {datetime.datetime.now()}")

# calculate FORMAT-level PL
print(f"...calculating FORMAT-level PL")
mt = mt.annotate_entries(PL=hl.case()
                            .when(mt.GT.is_hom_ref(), 
                                [0, mt.GQ, hl.max(mt.GQ, 3*(mt.AD[0] - mt.AD[1]))] )
                            .when(mt.GT.is_hom_var(), 
                                [hl.max(mt.GQ, 3*(mt.AD[0] - mt.AD[1])), mt.GQ, 0] )
                            .when(mt.GT.is_het(), 
                                [mt.GQ, 0, hl.max(mt.GQ, 3*(mt.AD[0] - mt.AD[1]))] )
                            .default(hl.array([hl.missing('int')])))
header['format']['PL'] = {'Description': 'Normalized, Phred-scaled likelihoods for genotypes as defined in the VCF specification (estimated using GQ).', 'Number': 'G', 'Type': 'Integer'}
print(f"...FORMAT-level PL calculations completed at: {datetime.datetime.now()}")

hl.export_vcf(mt, output_vcf, metadata=header, tabix=True)
print(f"...VCF export completed at: {datetime.datetime.now()}")
print("VCF reannotated with DP & PL: ", output_vcf)