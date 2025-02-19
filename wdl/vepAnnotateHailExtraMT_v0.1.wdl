version 1.0
    
import "scatterVCF.wdl" as scatterVCF
import "mergeSplitVCF.wdl" as mergeSplitVCF
import "mergeVCFs.wdl" as mergeVCFs
import "helpers.wdl" as helpers

struct RuntimeAttr {
    Float? mem_gb
    Int? cpu_cores
    Int? disk_gb
    Int? boot_disk_gb
    Int? preemptible_tries
    Int? max_retries
}

workflow vepAnnotateHailExtraMT {
    input {
        Array[String] vep_mt_uris

        File revel_file
        File clinvar_vcf_uri
        File omim_uri
        String mpc_ht_uri
        String loeuf_v2_uri
        String loeuf_v4_uri

        String bucket_id
        String cohort_prefix
        String hail_docker
        
        String vep_annotate_hail_extra_mt_python_script = "https://raw.githubusercontent.com/talkowski-lab/annotations/refs/heads/main/scripts/vep_annotate_hail_extra_mt_v0.1_dev.py"

        String genome_build='GRCh38'

        String spliceAI_uri='NA'
        String noncoding_bed='NA'
        String gene_list='NA'

        RuntimeAttr? runtime_attr_annotate_noncoding
        RuntimeAttr? runtime_attr_annotate_extra
        RuntimeAttr? runtime_attr_annotate_spliceAI
    }

    scatter (mt_uri in vep_mt_uris) {
        call helpers.getHailMTSize as getInputMTSize {
            input:
                mt_uri=mt_uri,
                hail_docker=hail_docker
        }

        if (noncoding_bed!='NA') {
            call annotateFromBed as annotateNonCoding {
                input:
                    mt_uri=mt_uri,
                    input_size=getInputMTSize.mt_size,
                    noncoding_bed=select_first([noncoding_bed]),
                    hail_docker=hail_docker,
                    genome_build=genome_build,
                    bucket_id=bucket_id,
                    filter=false,
                    runtime_attr_override=runtime_attr_annotate_noncoding
            }
        }

        call annotateExtra {
            input:
                mt_uri=select_first([annotateNonCoding.noncoding_mt, mt_uri]),
                input_size=getInputMTSize.mt_size,
                vep_annotate_hail_extra_mt_python_script=vep_annotate_hail_extra_mt_python_script,
                loeuf_v2_uri=loeuf_v2_uri,
                loeuf_v4_uri=loeuf_v4_uri,
                revel_file=revel_file,
                revel_file_idx=revel_file+'.tbi',
                clinvar_vcf_uri=clinvar_vcf_uri,
                omim_uri=omim_uri,
                gene_list=select_first([gene_list, 'NA']),
                mpc_ht_uri=mpc_ht_uri,
                bucket_id=bucket_id,
                hail_docker=hail_docker,
                genome_build=genome_build,
                runtime_attr_override=runtime_attr_annotate_extra
        }

        if (spliceAI_uri!='NA') {
            call annotateSpliceAI {
                input:
                    mt_uri=annotateExtra.annot_mt_uri,
                    input_size=getInputMTSize.mt_size,
                    spliceAI_uri=spliceAI_uri,
                    bucket_id=bucket_id,
                    genome_build=genome_build,
                    hail_docker=hail_docker,
                    runtime_attr_override=runtime_attr_annotate_spliceAI
            }
        }
        String annot_mt_uri = select_first([annotateSpliceAI.annot_mt_uri, annotateExtra.annot_mt_uri])
    }

    output {
        Array[String] annot_vep_mt_uris = annot_mt_uri
    }
}

task annotateFromBed {
    input {
        String mt_uri
        Float input_size
        String noncoding_bed 
        String hail_docker
        String genome_build
        String bucket_id
        Boolean filter
        RuntimeAttr? runtime_attr_override
    }
    
    Float base_disk_gb = 10.0
    Float input_disk_scale = 5.0

    RuntimeAttr runtime_default = object {
        mem_gb: 4,
        disk_gb: ceil(base_disk_gb + input_size * input_disk_scale),
        cpu_cores: 1,
        preemptible_tries: 3,
        max_retries: 1,
        boot_disk_gb: 10
    }

    RuntimeAttr runtime_override = select_first([runtime_attr_override, runtime_default])

    Float memory = select_first([runtime_override.mem_gb, runtime_default.mem_gb])
    Int cpu_cores = select_first([runtime_override.cpu_cores, runtime_default.cpu_cores])
    
    runtime {
        memory: "~{memory} GB"
        disks: "local-disk ~{select_first([runtime_override.disk_gb, runtime_default.disk_gb])} HDD"
        cpu: cpu_cores
        preemptible: select_first([runtime_override.preemptible_tries, runtime_default.preemptible_tries])
        maxRetries: select_first([runtime_override.max_retries, runtime_default.max_retries])
        docker: hail_docker
        bootDiskSizeGb: select_first([runtime_override.boot_disk_gb, runtime_default.boot_disk_gb])
    }

    command <<<
    cat <<EOF > annotate_noncoding.py
    from pyspark.sql import SparkSession
    import hail as hl
    import numpy as np
    import sys
    import ast
    import os
    import datetime
    import pandas as pd

    mt_uri = sys.argv[1]
    noncoding_bed = sys.argv[2]
    cores = sys.argv[3]
    mem = int(np.floor(float(sys.argv[4])))
    build = sys.argv[5]
    bucket_id = sys.argv[6]
    filter = ast.literal_eval(sys.argv[7].capitalize())

    hl.init(min_block_size=128, 
            local=f"local[*]", 
            spark_conf={
                        "spark.driver.memory": f"{int(np.floor(mem*0.8))}g",
                        "spark.speculation": 'true'
                        }, 
            tmp_dir="tmp", local_tmpdir="tmp",
                        )

    bed = hl.import_bed(noncoding_bed, reference_genome=build, skip_invalid_intervals=True)
    mt = hl.read_matrix_table(mt_uri)
    mt = mt.annotate_rows(info=mt.info.annotate(PREDICTED_NONCODING=bed[mt.locus].target))

    # filter only annotated
    if filter:
        mt = mt.filter_rows(hl.is_defined(mt.info.PREDICTED_NONCODING))

    filename = f"{bucket_id}/{str(datetime.datetime.now().strftime('%Y-%m-%d-%H-%M'))}/{os.path.basename(mt_uri).split('_vep.mt')[0]}_noncoding_annot.mt"
    mt.write(filename, overwrite=True)
    pd.Series([filename]).to_csv('noncoding_mt.txt', index=False, header=None)
    EOF
    python3 annotate_noncoding.py ~{mt_uri} ~{noncoding_bed} ~{cpu_cores} ~{memory} ~{genome_build} ~{bucket_id} ~{filter}
    cp $(ls . | grep hail*.log) hail_log.txt
    >>>

    output {
        String noncoding_mt = read_lines('noncoding_mt.txt')[0]
        File hail_log = "hail_log.txt"
    }
}

task annotateExtra {
    input {
        String mt_uri
        Float input_size
        String loeuf_v2_uri
        String loeuf_v4_uri
        File revel_file
        File revel_file_idx
        File clinvar_vcf_uri
        File omim_uri
        
        String gene_list
        String mpc_ht_uri

        String bucket_id
        String hail_docker
        String genome_build
        String vep_annotate_hail_extra_mt_python_script
        RuntimeAttr? runtime_attr_override
    }

    Float base_disk_gb = 10.0
    Float input_disk_scale = 10.0
    RuntimeAttr runtime_default = object {
        mem_gb: 8,
        disk_gb: ceil(base_disk_gb + input_size * input_disk_scale),
        cpu_cores: 1,
        preemptible_tries: 3,
        max_retries: 1,
        boot_disk_gb: 10
    }

    RuntimeAttr runtime_override = select_first([runtime_attr_override, runtime_default])
    Float memory = select_first([runtime_override.mem_gb, runtime_default.mem_gb])
    Int cpu_cores = select_first([runtime_override.cpu_cores, runtime_default.cpu_cores])
    
    runtime {
        memory: "~{memory} GB"
        disks: "local-disk ~{select_first([runtime_override.disk_gb, runtime_default.disk_gb])} HDD"
        cpu: cpu_cores
        preemptible: select_first([runtime_override.preemptible_tries, runtime_default.preemptible_tries])
        maxRetries: select_first([runtime_override.max_retries, runtime_default.max_retries])
        docker: hail_docker
        bootDiskSizeGb: select_first([runtime_override.boot_disk_gb, runtime_default.boot_disk_gb])
    }

    command <<<
        curl ~{vep_annotate_hail_extra_mt_python_script} > annotate.py
        python3 annotate.py -i ~{mt_uri} --cores ~{cpu_cores} --mem ~{memory} \
        --build ~{genome_build} --loeuf-v2 ~{loeuf_v2_uri} --loeuf-v4 ~{loeuf_v4_uri} \
        --mpc ~{mpc_ht_uri} --clinvar ~{clinvar_vcf_uri} --omim ~{omim_uri} \
        --revel ~{revel_file} --genes ~{gene_list} --bucket-id ~{bucket_id}
        cp $(ls . | grep hail*.log) hail_log.txt
    >>>

    output {
        String annot_mt_uri = read_lines('annot_mt.txt')[0]
        File hail_log = "hail_log.txt"
    }
}

task annotateSpliceAI {
    input {
        String mt_uri
        Float input_size
        String spliceAI_uri

        String bucket_id
        String hail_docker
        String genome_build
        RuntimeAttr? runtime_attr_override
    }

    Float base_disk_gb = 10.0
    Float input_disk_scale = 10.0
    RuntimeAttr runtime_default = object {
        mem_gb: 8,
        disk_gb: ceil(base_disk_gb + input_size * input_disk_scale),
        cpu_cores: 1,
        preemptible_tries: 3,
        max_retries: 1,
        boot_disk_gb: 10
    }

    RuntimeAttr runtime_override = select_first([runtime_attr_override, runtime_default])
    Float memory = select_first([runtime_override.mem_gb, runtime_default.mem_gb])
    Int cpu_cores = select_first([runtime_override.cpu_cores, runtime_default.cpu_cores])
    
    runtime {
        memory: "~{memory} GB"
        disks: "local-disk ~{select_first([runtime_override.disk_gb, runtime_default.disk_gb])} HDD"
        cpu: cpu_cores
        preemptible: select_first([runtime_override.preemptible_tries, runtime_default.preemptible_tries])
        maxRetries: select_first([runtime_override.max_retries, runtime_default.max_retries])
        docker: hail_docker
        bootDiskSizeGb: select_first([runtime_override.boot_disk_gb, runtime_default.boot_disk_gb])
    }

    command <<<
    cat <<EOF > annotate.py
    from pyspark.sql import SparkSession
    import hail as hl
    import numpy as np
    import pandas as pd
    import sys
    import ast
    import os
    import json
    import argparse
    import datetime

    parser = argparse.ArgumentParser(description='Parse arguments')
    parser.add_argument('-i', dest='mt_uri', help='Input MT file')
    parser.add_argument('--cores', dest='cores', help='CPU cores')
    parser.add_argument('--mem', dest='mem', help='Memory')
    parser.add_argument('--build', dest='build', help='Genome build')
    parser.add_argument('--spliceAI-uri', dest='spliceAI_uri', help='SpliceAI scores SNV/Indel HT')
    parser.add_argument('--bucket-id', dest='bucket_id', help='Google Bucket ID')

    args = parser.parse_args()

    mt_uri = args.mt_uri
    cores = args.cores  # string
    mem = int(np.floor(float(args.mem)))
    build = args.build
    spliceAI_uri = args.spliceAI_uri
    bucket_id = args.bucket_id

    hl.init(min_block_size=128, 
            local=f"local[*]", 
            spark_conf={
                        "spark.driver.memory": f"{int(np.floor(mem*0.8))}g",
                        "spark.speculation": 'true'
                        }, 
            tmp_dir="tmp", local_tmpdir="tmp",
                        )

    mt = hl.read_matrix_table(mt_uri)
    csq_columns = mt.globals.collect()[0].vep_csq_header.rsplit(" ", 1)[-1].split('|')

    # split VEP CSQ string
    mt = mt.annotate_rows(vep=mt.info)
    transcript_consequences = mt.vep.CSQ.map(lambda x: x.split('\|'))

    transcript_consequences_strs = transcript_consequences.map(lambda x: hl.if_else(hl.len(x)>1, hl.struct(**
                                                           {col: x[i] if col!='Consequence' else x[i].split('&')  
                                                            for i, col in enumerate(csq_columns)}), 
                                                            hl.struct(**{col: hl.missing('str') if col!='Consequence' else hl.array([hl.missing('str')])  
                                                            for i, col in enumerate(csq_columns)})))

    mt = mt.annotate_rows(vep=mt.vep.annotate(transcript_consequences=transcript_consequences_strs))
    mt = mt.annotate_rows(vep=mt.vep.select('transcript_consequences'))

    # annotate SpliceAI scores
    mt_by_transcript = mt.explode_rows(mt.vep.transcript_consequences)
    mt_by_locus_and_gene = mt_by_transcript.key_rows_by('locus', 'alleles', mt_by_transcript.vep.transcript_consequences.SYMBOL)

    spliceAI_ht = hl.read_table(spliceAI_uri)
    # leave out ALLELE/SYMBOL because redundant
    fields = 'ALLELE|SYMBOL|DS_AG|DS_AL|DS_DG|DS_DL|DP_AG|DP_AL|DP_DG|DP_DL'.split('|')[2:]  
    mt_by_locus_and_gene = mt_by_locus_and_gene.annotate_rows(SpliceAI_raw=spliceAI_ht[mt_by_locus_and_gene.row_key].SpliceAI)
    mt_by_locus_and_gene = mt_by_locus_and_gene.annotate_rows(vep=mt_by_locus_and_gene.vep.annotate(
        transcript_consequences=(mt_by_locus_and_gene.vep.transcript_consequences.annotate(
            **{field: hl.if_else(hl.is_defined(mt_by_locus_and_gene.SpliceAI_raw), 
                                mt_by_locus_and_gene.SpliceAI_raw.split('=')[1].split('\|')[i+2], '') 
            for i, field in enumerate(fields)}))))
    csq_fields_str = 'Format: ' + '|'.join(csq_columns) + '|'.join([''] + fields)

    mt_by_gene = mt_by_locus_and_gene
    mt_by_gene = (mt_by_gene.group_rows_by(mt_by_gene.locus, mt_by_gene.alleles)
        .aggregate_rows(vep = hl.agg.collect(mt_by_gene.vep))).result()

    fields = list(mt_by_gene.vep.transcript_consequences[0])
    new_csq = mt_by_gene.vep.transcript_consequences.scan(lambda i, j: i.extend([hl.str('|').join(
                                                          hl.array([j[col] if col != 'Consequence' else 
                                                                    hl.str('&').join(j[col]) 
                                                                    for col in list(fields)]))]), 
                                                                    hl.empty_array(hl.tstr))[-1]
                                                                
    mt_by_gene = mt_by_gene.annotate_rows(CSQ=new_csq)
    mt = mt.annotate_rows(info=mt.info.annotate(CSQ=mt_by_gene.rows()[mt.row_key].CSQ))
    mt = mt.drop('vep')

    filename = f"{bucket_id}/{str(datetime.datetime.now().strftime('%Y-%m-%d-%H-%M'))}/{os.path.basename(mt_uri).split('.mt')[0]}.SpliceAI_annot.mt"
    mt.write(filename, overwrite=True)
    pd.Series([filename]).to_csv('spliceAI_mt.txt', index=False, header=None)
    EOF
    python3 annotate.py -i ~{mt_uri} --cores ~{cpu_cores} --mem ~{memory} \
    --build ~{genome_build} --spliceAI-uri ~{spliceAI_uri} --bucket-id ~{bucket_id}
    cp $(ls . | grep hail*.log) hail_log.txt
    >>>

    output {
        String annot_mt_uri = read_lines('spliceAI_mt.txt')[0]
        File hail_log = "hail_log.txt"
    }
}