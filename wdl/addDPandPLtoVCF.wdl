version 1.0

struct RuntimeAttr {
    Float? mem_gb
    Int? cpu_cores
    Int? disk_gb
    Int? boot_disk_gb
    Int? preemptible_tries
    Int? max_retries
}

workflow addDPandPLtoVCF {
    input {
        # File vcf_shard
        Array[File] vcf_shards
        String add_dp_and_pl_python_script
        String hail_docker
        String genome_build='GRCh38'
        RuntimeAttr? runtime_attr_override
    }

    scatter (vcf_shard in vcf_shards) {
        call addDPandPL {
        input:
            vcf_shard=vcf_shard,
            add_dp_and_pl_python_script=add_dp_and_pl_python_script,
            hail_docker=hail_docker,
            genome_build=genome_build,
            runtime_attr_override=runtime_attr_override
        }
    }

    output {
        # File output_vcf = addDPandPL.output_vcf
        # File output_vcf_idx = addDPandPL.output_vcf_idx
        Array[File] reheadered_vcf_files = addDPandPL.reheadered_vcf
        Array[File] reheadered_vcf_indices = addDPandPL.reheadered_vcf_index
    }
}

task addDPandPL {
    input {
        File vcf_shard
        String add_dp_and_pl_python_script
        String hail_docker
        String genome_build
        RuntimeAttr? runtime_attr_override
    }
    Float input_size = size(vcf_shard, 'GB')
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

    String output_filename = sub(basename(vcf_shard), ".vcf.bgz", "") + ".DP.PL.vcf.bgz"

    # String input_basename = basename(vcf_shard)
    # String output_extension = if (sub(input_basename, "\\.vcf\\.bgz$", "") != input_basename) ".DP.vcf.bgz" else if (sub(input_basename, "\\.vcf\\.gz$", "") != input_basename) ".DP.vcf.gz" else ""
    # String output_extension = select_first([if sub(input_basename, "\\.vcf\\.bgz$", "") != input_basename then ".DP.PL.vcf.bgz" else None,
                                            # if sub(input_basename, "\\.vcf\\.gz$", "") != input_basename then ".DP.PL.vcf.gz" else None])

    # String output_filename = sub(input_basename, "\\.vcf\\.(bgz|gz)$", "") + output_extension

    # String filename = basename(vcf_shard)
    # String prefix = if (sub(filename, "\\.gz", "")!=filename) then basename(vcf_shard, ".vcf.gz") else basename(vcf_shard, ".vcf.bgz")

    command <<<
        set -eou pipefail
        curl ~{add_dp_and_pl_python_script} > add_dp_and_pl.py
        python3 add_dp_and_pl.py ~{vcf_shard} ~{output_filename} ~{cpu_cores} ~{memory} ~{genome_build}
        cp $(ls . | grep hail*.log) hail_log.txt
    >>>

    output {
        File reheadered_vcf = output_filename
        File reheadered_vcf_index = output_filename + '.tbi'
        File hail_log = "hail_log.txt"
    }
}