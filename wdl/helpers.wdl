version 1.0

struct RuntimeAttr {
    Float? mem_gb
    Int? cpu_cores
    Int? disk_gb
    Int? boot_disk_gb
    Int? preemptible_tries
    Int? max_retries
}

task addGenotypes {
    input {
        File annot_vcf_file
        File vcf_file
        String hail_docker
        String genome_build
        RuntimeAttr? runtime_attr_override
    }

    Float input_size = size([annot_vcf_file, vcf_file], "GB") 
    Float base_disk_gb = 10.0

    RuntimeAttr runtime_default = object {
                                      mem_gb: 4,
                                      disk_gb: ceil(base_disk_gb + input_size * 5.0),
                                      cpu_cores: 1,
                                      preemptible_tries: 3,
                                      max_retries: 1,
                                      boot_disk_gb: 10
                                  }
    RuntimeAttr runtime_override = select_first([runtime_attr_override, runtime_default])
    Float memory = select_first([runtime_override.mem_gb, runtime_default.mem_gb])
    Int cpu_cores = select_first([runtime_override.cpu_cores, runtime_default.cpu_cores])
    
    runtime {
        memory: "~{select_first([runtime_override.mem_gb, runtime_default.mem_gb])} GB"
        disks: "local-disk ~{select_first([runtime_override.disk_gb, runtime_default.disk_gb])} HDD"
        cpu: select_first([runtime_override.cpu_cores, runtime_default.cpu_cores])
        preemptible: select_first([runtime_override.preemptible_tries, runtime_default.preemptible_tries])
        maxRetries: select_first([runtime_override.max_retries, runtime_default.max_retries])
        docker: hail_docker
        bootDiskSizeGb: select_first([runtime_override.boot_disk_gb, runtime_default.boot_disk_gb])
    }

    String filename = basename(annot_vcf_file)
    String prefix = if (sub(filename, "\\.gz", "")!=filename) then basename(annot_vcf_file, ".vcf.gz") else basename(annot_vcf_file, ".vcf.bgz")
    String combined_vcf_name = "~{prefix}.GT.vcf.bgz"

    command <<<
    cat <<EOF > merge_vcfs.py
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
    parser.add_argument('-i', dest='vcf_file', help='Input VCF file before annotation')
    parser.add_argument('-a', dest='annot_vcf_file', help='Input VCF file after annotation')
    parser.add_argument('-o', dest='combined_vcf_name', help='Output filename')
    parser.add_argument('--cores', dest='cores', help='CPU cores')
    parser.add_argument('--mem', dest='mem', help='Memory')
    parser.add_argument('--build', dest='build', help='Genome build')

    args = parser.parse_args()

    vcf_file = args.vcf_file
    annot_vcf_file = args.annot_vcf_file
    combined_vcf_name = args.combined_vcf_name
    cores = args.cores  # string
    mem = int(np.floor(float(args.mem)))
    build = args.build

    hl.init(min_block_size=128, 
            local=f"local[*]", 
            spark_conf={
                        "spark.driver.memory": f"{int(np.floor(mem*0.8))}g",
                        "spark.speculation": 'true'
                        }, 
            tmp_dir="tmp", local_tmpdir="tmp",
                        )

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

    mt = hl.import_vcf(vcf_file, force_bgz=True, array_elements_required=False, call_fields=[], reference_genome=build)
    mt = split_multi_ssc(mt)
    try:
        # for haploid (e.g. chrY)
        mt = mt.annotate_entries(
            GT = hl.if_else(
                    mt.GT.ploidy == 1, 
                    hl.call(mt.GT[0], mt.GT[0]),
                    mt.GT)
        )
    except:
        pass

    header = hl.get_vcf_metadata(annot_vcf_file) 
    annot_mt = hl.import_vcf(annot_vcf_file, force_bgz=True, array_elements_required=False, call_fields=[], reference_genome=build)

    annot_mt = annot_mt.union_cols(mt)

    hl.export_vcf(dataset=annot_mt, output=combined_vcf_name, metadata=header, tabix=True)
    EOF

    python3 merge_vcfs.py -i ~{vcf_file} -a ~{annot_vcf_file} -o ~{combined_vcf_name} --cores ~{cpu_cores} --mem ~{memory} \
        --build ~{genome_build}
    >>>

    output {
        File combined_vcf_file = combined_vcf_name
        File combined_vcf_idx = combined_vcf_name + ".tbi"
    }

}

task splitFileWithHeader {
    input {
        File file
        Int shards_per_chunk
        String cohort_prefix
        String hail_docker
        RuntimeAttr? runtime_attr_override
    }
    Float input_size = size(file, "GB")
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
    cat <<EOF > split_file.py
    import pandas as pd
    import numpy as np
    import os
    import sys

    file = sys.argv[1]
    shards_per_chunk = int(sys.argv[2])

    file_ext = file.split('.')[-1]
    base_filename = os.path.basename(file).split(f".{file_ext}")[0]
    pd.Series([base_filename]).to_csv('base_filename.txt', index=False, header=None)
    df = pd.read_csv(file, sep='\t')
    n_chunks = np.ceil(df.shape[0]/shards_per_chunk)
    i=0
    while i<n_chunks:
        filename = f"{base_filename}.shard_{i}.{file_ext}"
        df.iloc[i*shards_per_chunk:(i+1)*shards_per_chunk, :].to_csv(filename, sep='\t', index=False)
        i+=1
    
    EOF
    python3 split_file.py ~{file} ~{shards_per_chunk} > stdout
    >>>

    String base_filename = read_lines('base_filename.txt')
    output {
        Array[File] chunks = glob("~{base_filename}.shard_*")
    }
}

task splitSamples {
    input {
        File vcf_file
        Int samples_per_chunk
        String cohort_prefix
        String sv_base_mini_docker
        RuntimeAttr? runtime_attr_override
    }

    Float input_size = size(vcf_file, "GB")
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
        docker: sv_base_mini_docker
        bootDiskSizeGb: select_first([runtime_override.boot_disk_gb, runtime_default.boot_disk_gb])
    }

    command <<<
        set -eou pipefail
        bcftools query -l ~{vcf_file} > ~{cohort_prefix}_samples.txt

        cat <<EOF > split_samples.py 
        import os
        import sys
        import pandas as pd
        import numpy as np

        cohort_prefix = sys.argv[1]
        samples_per_chunk = int(sys.argv[2])

        samples = sorted(pd.read_csv(f"{cohort_prefix}_samples.txt", header=None)[0].tolist())
        n = samples_per_chunk  # number of samples in each chunk
        chunks = [samples[i * n:(i + 1) * n] for i in range((len(samples) + n - 1) // n )]  
        
        shard_samples = []
        for i, chunk1 in enumerate(chunks):
            for chunk2 in chunks[i+1:]:
                shard_samples.append(chunk1 + chunk2)

        for i, shard in enumerate(shard_samples):
            pd.Series(shard).to_csv(f"{cohort_prefix}_shard_{i}.txt", index=False, header=None)
        EOF

        python3 split_samples.py ~{cohort_prefix} ~{samples_per_chunk}
    >>>

    output {
        Array[File] sample_shard_files = glob("~{cohort_prefix}_shard_*.txt")
    }
}

task splitFamilies {
    input {
        File ped_uri
        Int families_per_chunk
        String cohort_prefix
        String sv_base_mini_docker
        RuntimeAttr? runtime_attr_override
    }

    Float input_size = size(ped_uri, "GB")
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
        docker: sv_base_mini_docker
        bootDiskSizeGb: select_first([runtime_override.boot_disk_gb, runtime_default.boot_disk_gb])
    }

    command <<<
        set -eou pipefail

        cat <<EOF > split_samples.py 
        import os
        import sys
        import pandas as pd
        import numpy as np

        cohort_prefix = sys.argv[1]
        families_per_chunk = int(sys.argv[2])  # approximation for trios
        ped_uri = sys.argv[3]

        ped = pd.read_csv(ped_uri, sep='\t').iloc[:, :6]
        ped.columns = ['family_id', 'sample_id', 'paternal_id', 'maternal_id', 'sex', 'phenotype']

        n = families_per_chunk
        families = ped.family_id.unique().tolist()

        chunks = [families[i * n:(i + 1) * n] for i in range((len(families) + n - 1) // n )]  
        sample_chunks = [ped[ped.family_id.isin(fams)].sample_id.to_list() for fams in chunks]

        for i, shard in enumerate(sample_chunks):
            pd.Series(shard).to_csv(f"{cohort_prefix}_shard_{i}.txt", index=False, header=None)
        EOF

        python3 split_samples.py ~{cohort_prefix} ~{families_per_chunk} ~{ped_uri}
    >>>

    output {
        Array[File] family_shard_files = glob("~{cohort_prefix}_shard_*.txt")
    }
}

task mergeResultsPython {
     input {
        Array[String] tsvs
        String hail_docker
        String merged_filename
        Float input_size
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
        cat <<EOF > merge_tsvs.py
        import pandas as pd
        import numpy as np
        import sys

        tsvs = pd.read_csv(sys.argv[1], header=None)[0].tolist()
        merged_filename = sys.argv[2]

        dfs = []
        tot = len(tsvs)
        merged_df = pd.DataFrame()
        for i, tsv in enumerate(tsvs):
            if (i+1)%100==0:
                print(f"Loading tsv {i+1}/{tot}...")
            df = pd.read_csv(tsv, sep='\t')
            merged_df = pd.concat([merged_df, df])
        merged_df.to_csv(merged_filename, sep='\t', index=False)
        EOF

        python3 merge_tsvs.py ~{write_lines(tsvs)} ~{merged_filename} > stdout
    >>>

    output {
        File merged_tsv = merged_filename 
    }   
}

task mergeResults {
    input {
        Array[File] tsvs
        String hail_docker
        String merged_filename
        RuntimeAttr? runtime_attr_override
    }

    Float input_size = size(tsvs, "GB")
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
        head -n 1 ~{tsvs[0]} > ~{merged_filename}; 
        tail -n +2 -q ~{sep=' ' tsvs} >> ~{merged_filename}
    >>>

    output {
        File merged_tsv = merged_filename
    }
}

task getHailMTSize {
    input {
        String mt_uri
        String hail_docker
        RuntimeAttr? runtime_attr_override
    }
    Float base_disk_gb = 10.0
    Float input_disk_scale = 5.0

    RuntimeAttr runtime_default = object {
        mem_gb: 4,
        disk_gb: ceil(base_disk_gb),
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
        tot_size=$(gsutil -m du -sh ~{mt_uri} | awk -F '    ' '{ print $1 }')

        cat <<EOF > convert_to_gb.py
        import sys
        size = sys.argv[1]
        unit = sys.argv[2]
        def convert_to_gib(size, unit):
            size_dict = {"KiB": 2**10, "MiB": 2**20, "GiB": 2**30, "TiB": 2**40}
            return float(size) * size_dict[unit] / size_dict["GiB"]
        size_in_gib = convert_to_gib(size, unit)
        print(size_in_gib)        
        EOF

        python3 convert_to_gb.py $tot_size > mt_size.txt
    >>>

    output {
        Float mt_size = read_lines('mt_size.txt')[0]
    }
}

task getHailMTSizes {
    input {
        Array[String] mt_uris
        String hail_docker
        RuntimeAttr? runtime_attr_override
    }
    Float base_disk_gb = 10.0
    Float input_disk_scale = 5.0

    RuntimeAttr runtime_default = object {
        mem_gb: 4,
        disk_gb: ceil(base_disk_gb),
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
        tot_size=$(gsutil -m du -shc ~{sep=' ' mt_uris} | awk -F '    ' '{ print $1 }' | tail -n 1)

        cat <<EOF > convert_to_gb.py
        import sys
        size = sys.argv[1]
        unit = sys.argv[2]
        def convert_to_gib(size, unit):
            size_dict = {"KiB": 2**10, "MiB": 2**20, "GiB": 2**30}
            return float(size) * size_dict[unit] / size_dict["GiB"]
        size_in_gib = convert_to_gib(size, unit)
        print(size_in_gib)        
        EOF

        python3 convert_to_gb.py $tot_size > tot_size.txt
    >>>

    output {
        Float mt_size = read_lines('tot_size.txt')[0]
    }
}

task filterIntervalsToMT {
    input {
        File bed_file
        Float input_size
        String mt_uri
        String bucket_id        
        String hail_docker
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
    cat <<EOF > filter_intervals.py
    import datetime
    import pandas as pd
    import hail as hl
    import numpy as np
    import sys
    import os

    mt_uri = sys.argv[1]
    bed_file = sys.argv[2]
    cores = sys.argv[3]
    mem = int(np.floor(float(sys.argv[4])))
    bucket_id = sys.argv[5]

    hl.init(min_block_size=128, spark_conf={"spark.executor.cores": cores, 
                        "spark.executor.memory": f"{int(np.floor(mem*0.4))}g",
                        "spark.driver.cores": cores,
                        "spark.driver.memory": f"{int(np.floor(mem*0.4))}g"
                        }, tmp_dir="tmp", local_tmpdir="tmp")

    mt = hl.read_matrix_table(mt_uri)
    intervals = hl.import_bed(bed_file, reference_genome='GRCh38')
    mt_filt = mt.filter_rows(hl.is_defined(intervals[mt.locus]))

    filename = f"{bucket_id}/hail/{str(datetime.datetime.now().strftime('%Y-%m-%d_%H-%M'))}/{os.path.basename(mt_uri).split('.mt')[0]}_{os.path.basename(bed_file).split('.bed')[0]}.mt"
    mt_filt.write(filename)
    pd.Series([filename]).to_csv('mt_uri.txt', index=False, header=None)
    EOF
    
    python3 filter_intervals.py ~{mt_uri} ~{bed_file} ~{cpu_cores} ~{memory} ~{bucket_id}
    >>>

    output {
        String mt_filt = read_lines('mt_uri.txt')[0]
    }
}

task filterIntervalsToVCF {
    input {
        File bed_file
        Float input_size
        String mt_uri
        String hail_docker
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
    cat <<EOF > filter_intervals.py
    import datetime
    import pandas as pd
    import hail as hl
    import numpy as np
    import sys
    import os

    mt_uri = sys.argv[1]
    bed_file = sys.argv[2]
    cores = sys.argv[3]
    mem = int(np.floor(float(sys.argv[4])))

    hl.init(min_block_size=128, spark_conf={"spark.executor.cores": cores, 
                        "spark.executor.memory": f"{int(np.floor(mem*0.4))}g",
                        "spark.driver.cores": cores,
                        "spark.driver.memory": f"{int(np.floor(mem*0.4))}g"
                        }, tmp_dir="tmp", local_tmpdir="tmp")

    if mt_uri.split('.')[-1]=='mt':
        mt = hl.read_matrix_table(mt_uri)
    else:
        mt = hl.import_vcf(mt_uri, reference_genome='GRCh38', force_bgz=True, array_elements_required=False, call_fields=[])
    
    bed_df = pd.read_csv(bed_file, sep='\t')
    try:
        float(bed_df.columns[0]) 
        bed_df = pd.read_csv(bed_file, sep='\t', header=None)
    except:  # has header
        pass
    new_bed_file = f"{os.path.basename(bed_file).split('.bed')[0]}_no_header.bed"
    bed_df.iloc[:,:3].to_csv(new_bed_file, sep='\t', header=None, index=False)
    intervals = hl.import_bed(new_bed_file, reference_genome='GRCh38')
    mt_filt = mt.filter_rows(hl.is_defined(intervals[mt.locus]))

    filename = f"{os.path.basename(mt_uri).split('.mt')[0]}_{os.path.basename(bed_file).split('.bed')[0]}.vcf.bgz"
    hl.export_vcf(mt_filt, filename)
    EOF
    
    python3 filter_intervals.py ~{mt_uri} ~{bed_file} ~{cpu_cores} ~{memory}
    >>>

    output {
        File vcf_filt = "~{basename(mt_uri, '.mt')}_~{basename(bed_file, '.bed')}.vcf.bgz"
    }
}

task subsetVCFSamplesHail {
    input {
        File vcf_file
        File samples_file  # .txt extension  
        String hail_docker
        String genome_build='GRCh38'
        RuntimeAttr? runtime_attr_override
    }
    Float input_size = size(vcf_file, 'GB')
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
    cat <<EOF > subset_samples.py
    import datetime
    import pandas as pd
    import hail as hl
    import numpy as np
    import sys
    import os

    vcf_file = sys.argv[1]
    samples_file = sys.argv[2]
    cores = sys.argv[3]
    mem = int(np.floor(float(sys.argv[4])))
    genome_build = sys.argv[5]

    hl.init(min_block_size=128, spark_conf={"spark.executor.cores": cores, 
                        "spark.executor.memory": f"{int(np.floor(mem*0.4))}g",
                        "spark.driver.cores": cores,
                        "spark.driver.memory": f"{int(np.floor(mem*0.4))}g"
                        }, tmp_dir="tmp", local_tmpdir="tmp")

    mt = hl.import_vcf(vcf_file, reference_genome = genome_build, array_elements_required=False, force_bgz=True)
    header = hl.get_vcf_metadata(vcf_file)

    samples = pd.read_csv(samples_file, header=None)[0].astype(str).tolist()
    try:
        # for haploid (e.g. chrY)
        mt = mt.annotate_entries(
            GT = hl.if_else(
                    mt.GT.ploidy == 1, 
                    hl.call(mt.GT[0], mt.GT[0]),
                    mt.GT)
        )
    except:
        pass

    mt_filt = mt.filter_cols(hl.array(samples).contains(mt.s))
    mt_filt = hl.variant_qc(mt_filt)
    mt_filt = mt_filt.filter_rows(mt_filt.variant_qc.AC[1]>0)
    hl.export_vcf(mt_filt, os.path.basename(samples_file).split('.txt')[0]+'.vcf.bgz', metadata=header)
    EOF

    python3 subset_samples.py ~{vcf_file} ~{samples_file} ~{cpu_cores} ~{memory} ~{genome_build}
    >>>

    output {
        File vcf_subset = basename(samples_file, '.txt') + '.vcf.bgz'
    }
}

task mergeMTs {
    input {
        Array[String] mt_uris
        String cohort_prefix
        String bucket_id
        String hail_docker
        RuntimeAttr? runtime_attr_override
    }

    Float base_disk_gb = 10.0
    Float input_disk_scale = 5.0

    RuntimeAttr runtime_default = object {
        mem_gb: 4,
        disk_gb: ceil(base_disk_gb),
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
    cat <<EOF > merge_mts.py
    import datetime
    import pandas as pd
    import hail as hl
    import numpy as np
    import sys
    import os

    mt_uris = sys.argv[1].split(',')
    merged_filename = sys.argv[2]
    cores = sys.argv[3]
    mem = int(np.floor(float(sys.argv[4])))
    bucket_id = sys.argv[5]

    hl.init(min_block_size=128, spark_conf={"spark.executor.cores": cores, 
                        "spark.executor.memory": f"{int(np.floor(mem*0.4))}g",
                        "spark.driver.cores": cores,
                        "spark.driver.memory": f"{int(np.floor(mem*0.4))}g"
                        }, tmp_dir="tmp", local_tmpdir="tmp")

    tot_mt = len(mt_uris)
    for i, mt_uri in enumerate(mt_uris):
        if (((i+1)%10)==0):
            print(f"Merging MT {i+1}/{tot_mt}...")
        if i==0:
            mt = hl.read_matrix_table(mt_uri)
        else:
            mt2 = hl.read_matrix_table(mt_uri)
            mt = mt.union_rows(mt2)
    filename = f"{bucket_id}/hail/merged_mt/{str(datetime.datetime.now().strftime('%Y-%m-%d_%H-%M'))}/{merged_filename}.mt"
    mt.write(filename, overwrite=True)
    pd.Series([filename]).to_csv('mt_uri.txt', index=False, header=None)
    EOF

    python3 merge_mts.py ~{sep=',' mt_uris} ~{cohort_prefix}_merged ~{cpu_cores} ~{memory} ~{bucket_id}
    >>>

    output {
        String merged_mt = read_lines('mt_uri.txt')[0]
    }
}

task mergeHTs {
    input {
        Array[String] ht_uris
        String merged_filename
        String hail_docker
        Float input_size
        RuntimeAttr? runtime_attr_override
    }

    Float base_disk_gb = 10.0
    Float input_disk_scale = 5.0

    RuntimeAttr runtime_default = object {
        mem_gb: 4,
        disk_gb: ceil(base_disk_gb),
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
    cat <<EOF > merge_hts.py
    import datetime
    import pandas as pd
    import hail as hl
    import numpy as np
    import sys
    import os

    ht_uris = sys.argv[1].split(',')
    merged_filename = sys.argv[2]
    cores = sys.argv[3]
    mem = int(np.floor(float(sys.argv[4])))

    hl.init(min_block_size=128, spark_conf={"spark.executor.cores": cores, 
                        "spark.executor.memory": f"{int(np.floor(mem*0.4))}g",
                        "spark.driver.cores": cores,
                        "spark.driver.memory": f"{int(np.floor(mem*0.4))}g"
                        }, tmp_dir="tmp", local_tmpdir="tmp")

    tot_ht = len(ht_uris)
    merged_df = pd.DataFrame()

    for i, uri in enumerate(ht_uris):
        print(f"{i+1}/{tot_ht}")
        ht = hl.read_table(uri)
        merged_df = pd.concat([merged_df, ht.to_pandas()])

    merged_df.to_csv(f"{merged_filename}.tsv", sep='\t', index=False)
    EOF

    python3 merge_hts.py ~{sep=',' ht_uris} ~{merged_filename} ~{cpu_cores} ~{memory}
    >>>

    output {
        File merged_tsv = merged_filename + '.tsv'
    }
}

task subsetVCFs {
    input {
        File vcf_uri
        File vcf_idx
        File bed_file
        String output_name
        String sv_base_mini_docker
        RuntimeAttr? runtime_attr_override
    }

    Float relatedness_size = size(vcf_uri, "GB") 
    Float base_disk_gb = 10.0
    RuntimeAttr runtime_default = object {
                                      mem_gb: 4,
                                      disk_gb: ceil(base_disk_gb + (relatedness_size) * 5.0),
                                      cpu_cores: 1,
                                      preemptible_tries: 3,
                                      max_retries: 1,
                                      boot_disk_gb: 10
                                  }
    RuntimeAttr runtime_override = select_first([runtime_attr_override, runtime_default])
    runtime {
        memory: "~{select_first([runtime_override.mem_gb, runtime_default.mem_gb])} GB"
        disks: "local-disk ~{select_first([runtime_override.disk_gb, runtime_default.disk_gb])} HDD"
        cpu: select_first([runtime_override.cpu_cores, runtime_default.cpu_cores])
        preemptible: select_first([runtime_override.preemptible_tries, runtime_default.preemptible_tries])
        maxRetries: select_first([runtime_override.max_retries, runtime_default.max_retries])
        docker: sv_base_mini_docker
        bootDiskSizeGb: select_first([runtime_override.boot_disk_gb, runtime_default.boot_disk_gb])
    }

    command {
        bcftools view -R ~{bed_file} ~{vcf_uri} -o ~{output_name}
        bcftools index -t ~{output_name}
    }

    output {
        File subset_vcf = output_name
        File subset_vcf_idx = output_name + '.tbi'
    }
}