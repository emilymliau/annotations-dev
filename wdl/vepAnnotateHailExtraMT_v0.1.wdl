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

workflow vepAnnotateHailExtra {

    input {
        Array[File] mt_files

        File revel_file
        File clinvar_vcf_uri
        File omim_uri
        String mpc_ht_uri
        String loeuf_v2_uri
        String loeuf_v4_uri

        String cohort_prefix
        String hail_docker
        
        String vep_annotate_hail_extra_python_script = "https://raw.githubusercontent.com/talkowski-lab/annotations/refs/heads/main/scripts/vep_annotate_hail_extra_v0.1_dev.py"
        String split_vcf_hail_script = "https://raw.githubusercontent.com/talkowski-lab/annotations/refs/heads/main/scripts/split_vcf_hail.py"

        String genome_build='GRCh38'

        String spliceAI_uri='NA'
        String noncoding_bed='NA'
        String gene_list='NA'

        RuntimeAttr? runtime_attr_annotate_noncoding      
        RuntimeAttr? runtime_attr_annotate_extra
        RuntimeAttr? runtime_attr_annotate_spliceAI
        RuntimeAttr? runtime_attr_annotate_add_genotypes
    }

    scatter (mt_shard in mt_files) {
        if (noncoding_bed!='NA') {
            call annotateFromBed as annotateNonCoding {
                input:
                mt_file=mt_shard,
                noncoding_bed=select_first([noncoding_bed]),
                hail_docker=hail_docker,
                genome_build=genome_build,
                filter=false,
                runtime_attr_override=runtime_attr_annotate_noncoding
            }
        }

        call annotateExtra {
            input:
                mt_file=select_first([annotateNonCoding.noncoding_mt, mt_shard]),
                vep_annotate_hail_extra_python_script=vep_annotate_hail_extra_python_script,
                loeuf_v2_uri=loeuf_v2_uri,
                loeuf_v4_uri=loeuf_v4_uri,
                revel_file=revel_file,
                clinvar_vcf_uri=clinvar_vcf_uri,
                omim_uri=omim_uri,
                gene_list=select_first([gene_list, 'NA']),
                mpc_ht_uri=mpc_ht_uri,
                hail_docker=hail_docker,
                genome_build=genome_build,
                runtime_attr_override=runtime_attr_annotate_extra
        }

        if (spliceAI_uri!='NA') {
            call annotateSpliceAI {
                input:
                mt_file=annotateExtra.annot_mt_file,
                spliceAI_uri=spliceAI_uri,
                genome_build=genome_build,
                hail_docker=hail_docker,
                runtime_attr_override=runtime_attr_annotate_spliceAI
            }
        }
        File annot_mt_file = select_first([annotateSpliceAI.annot_mt_file, annotateExtra.annot_mt_file])

        call helpers.addGenotypesMT as addGenotypesMT {
            input:
            annot_mt_file=annot_mt_file,
            mt_file=mt_shard,
            genome_build=genome_build,
            hail_docker=hail_docker,
            runtime_attr_override=runtime_attr_annotate_add_genotypes
        }
    }

    output {
        Array[File] annot_mt_files = addGenotypesMT.combined_mt_file
    }
}   

task annotateFromBed {
    input {
        File mt_file
        String noncoding_bed 
        String hail_docker
        String genome_build
        Boolean filter
        RuntimeAttr? runtime_attr_override
    }
    Float input_size = size(mt_file, 'GB')
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

    String output_filename = basename(mt_file, ".mt") + '_noncoding_annot.mt' 
   
    command <<<
    cat <<EOF > annotate_noncoding.py
    from pyspark.sql import SparkSession
    import hail as hl
    import numpy as np
    import sys
    import ast
    import os

    mt_file = sys.argv[1]
    noncoding_bed = sys.argv[2]
    cores = sys.argv[3]  # string
    mem = int(np.floor(float(sys.argv[4])))
    build = sys.argv[5]
    output_filename = sys.argv[6]
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
    mt = hl.read_matrix_table(mt_file)
    mt = mt.annotate_rows(info=mt.info.annotate(PREDICTED_NONCODING=bed[mt.locus].target))

    # filter only annotated
    if filter:
        mt = mt.filter_rows(hl.is_defined(mt.info.PREDICTED_NONCODING))

    mt.write(output_filename, overwrite=True)
    EOF
    python3 annotate_noncoding.py ~{mt_file} ~{noncoding_bed} ~{cpu_cores} ~{memory} ~{genome_build} ~{output_filename} ~{filter}
    >>>

    output {
        File noncoding_mt = output_filename
    }
}

task annotateExtra {
    input {
        File mt_file
        String loeuf_v2_uri
        String loeuf_v4_uri
        File revel_file
        File clinvar_vcf_uri
        File omim_uri
        
        String gene_list
        String mpc_ht_uri

        String hail_docker
        String genome_build
        String vep_annotate_hail_extra_python_script
        RuntimeAttr? runtime_attr_override
    }

    Float input_size = size(mt_file, "GB")
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

    String filename = basename(mt_file)
    String prefix = basename(mt_file, ".mt")
    String vep_annotated_mt_name = "~{prefix}.annot.mt"

    command <<<
        curl ~{vep_annotate_hail_extra_python_script} > annotate.py
        python3 annotate.py -i ~{mt_file} -o ~{vep_annotated_mt_name} --cores ~{cpu_cores} --mem ~{memory} \
        --build ~{genome_build} --loeuf-v2 ~{loeuf_v2_uri} --loeuf-v4 ~{loeuf_v4_uri} \
        --mpc ~{mpc_ht_uri} --clinvar ~{clinvar_vcf_uri} --omim ~{omim_uri} \
        --revel ~{revel_file} --genes ~{gene_list} 
        cp $(ls . | grep hail*.log) hail_log.txt
    >>>

    output {
        File annot_mt_file = vep_annotated_mt_name
        File hail_log = "hail_log.txt"
    }
}

task annotateSpliceAI {
    input {
        File mt_file
        String spliceAI_uri

        String hail_docker
        String genome_build
        RuntimeAttr? runtime_attr_override
    }

    Float input_size = size(mt_file, "GB")
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

    String filename = basename(mt_file)
    String prefix = basename(mt_file, ".mt")
    String vep_annotated_mt_name = "~{prefix}.SpliceAI.annot.mt"

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

    parser = argparse.ArgumentParser(description='Parse arguments')
    parser.add_argument('-i', dest='mt_file', help='Input MT file')
    parser.add_argument('-o', dest='vep_annotated_mt_name', help='Output filename')
    parser.add_argument('--cores', dest='cores', help='CPU cores')
    parser.add_argument('--mem', dest='mem', help='Memory')
    parser.add_argument('--build', dest='build', help='Genome build')
    parser.add_argument('--spliceAI-uri', dest='spliceAI_uri', help='SpliceAI scores SNV/Indel HT')

    args = parser.parse_args()

    mt_file = args.mt_file
    vep_annotated_mt_name = args.vep_annotated_mt_name
    cores = args.cores  # string
    mem = int(np.floor(float(args.mem)))
    build = args.build
    spliceAI_uri = args.spliceAI_uri

    hl.init(min_block_size=128, 
            local=f"local[*]", 
            spark_conf={
                        "spark.driver.memory": f"{int(np.floor(mem*0.8))}g",
                        "spark.speculation": 'true'
                        }, 
            tmp_dir="tmp", local_tmpdir="tmp",
                        )

    mt = hl.read_matrix_table(mt_file)

    csq_columns = mt.info.CSQ[0].split('|')
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
    new_csq = mt_by_gene.vep.transcript_consequences.scan(lambda i, j: 
                                        hl.str('|').join(hl.array([i]))
                                        +','+hl.str('|').join(hl.array([j[col] if col!='Consequence' else 
                                                                    hl.str('&').join(j[col]) 
                                                                    for col in list(fields)])), '')[-1][1:]
    mt_by_gene = mt_by_gene.annotate_rows(CSQ=new_csq)
    mt = mt.annotate_rows(info=mt.info.annotate(CSQ=mt_by_gene.rows()[mt.row_key].CSQ))
    mt = mt.drop('vep')

    mt.write(vep_annotated_mt_name, overwrite=True)
    EOF
    python3 annotate.py -i ~{mt_file} -o ~{vep_annotated_mt_name} --cores ~{cpu_cores} --mem ~{memory} \
    --build ~{genome_build} --spliceAI-uri ~{spliceAI_uri} 
    cp $(ls . | grep hail*.log) hail_log.txt
    >>>

    output {
        File annot_mt_file = vep_annotated_mt_name
        File hail_log = "hail_log.txt"
    }
}