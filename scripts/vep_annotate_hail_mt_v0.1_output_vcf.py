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

#split-multi
def split_multi_ssc(mt):
    mt = mt.annotate_rows(num_alleles = mt.alleles.size() ) # Add number of alleles at site before split
    # only split variants that aren't already split
    bi = mt.filter_rows(hl.len(mt.alleles) == 2)
    bi = bi.annotate_rows(a_index=1, was_split=False, old_locus=bi.locus, old_alleles=bi.alleles)
    multi = mt.filter_rows(hl.len(mt.alleles) > 2)
    # Now split
    split = hl.split_multi(multi, permit_shuffle=True)
    sm = split.union_rows(bi)
    # sm = hl.split_multi(mt, permit_shuffle=True)
    if 'PL' in list(mt.entry.keys()):
        pl = hl.or_missing(hl.is_defined(sm.PL),
                        (hl.range(0, 3).map(lambda i: hl.min(hl.range(0, hl.len(sm.PL))
        .filter(lambda j: hl.downcode(hl.unphased_diploid_gt_index_call(j), sm.a_index) == hl.unphased_diploid_gt_index_call(i))
        .map(lambda j: sm.PL[j])))))
        sm = sm.annotate_entries(PL = pl)
    split_ds = sm.annotate_entries(GT = hl.downcode(sm.GT, sm.a_index),
                                   AD = hl.or_missing(hl.is_defined(sm.AD), [hl.sum(sm.AD) - sm.AD[sm.a_index], sm.AD[sm.a_index]])
                                   ) 
        #GQ = hl.cond(hl.is_defined(pl[0]) & hl.is_defined(pl[1]) & hl.is_defined(pl[2]), hl.gq_from_pl(pl), sm.GQ) )
    mt = split_ds.drop('old_locus', 'old_alleles')
    return mt

mt = hl.read_matrix_table(mt_uri)
# mt = mt.distinct_by_row()

if 'num_alleles' not in list(mt.row_value.keys()):
    mt = split_multi_ssc(mt)
    # mt = mt.distinct_by_row()
# annotate cohort ac to INFO field (after splitting multiallelic)
if 'cohort_AC' not in list(mt.info):
    mt = mt.annotate_rows(info=mt.info.annotate(cohort_AC=mt.info.AC[mt.a_index - 1]))
if 'cohort_AF' not in list(mt.info):
    mt = mt.annotate_rows(info=mt.info.annotate(cohort_AF=mt.info.AF[mt.a_index - 1]))

if 'vep' in list(mt.row):
    mt = mt.annotate_rows(original_vep=mt.vep)
    
mt = hl.vep(mt, config='vep_config.json', csq=True, tolerate_parse_error=True)
mt = mt.annotate_rows(info = mt.info.annotate(CSQ=mt.vep))

# mt_filename = f"{bucket_id}/vep-annotate-hail-mt/{str(datetime.datetime.now().strftime('%Y-%m-%d_%H-%M'))}/{prefix}_vep.mt"
# pd.Series([mt_filename]).to_csv('mt_uri.txt',index=False, header=None)
# mt.write(mt_filename, overwrite=True)

# export MT as VCF
vcf_filename = f"{bucket_id}/vep-annotate-hail-mt/{str(datetime.datetime.now().strftime('%Y-%m-%d_%H-%M'))}/{prefix}_vep.vcf.bgz"
hl.export_vcf(mt, vcf_filename)
pd.Series([vcf_filename]).to_csv('vcf_uri.txt',index=False, header=None)