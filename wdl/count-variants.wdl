version 1.0

struct RuntimeAttr {
    Float? mem_gb
    Int? cpu_cores
    Int? disk_gb
    Int? boot_disk_gb
    Int? preemptible_tries
    Int? max_retries
}

workflow countVariants {
    input {
        # File input_vcf
        Array[File] vcf_shards
        String hail_docker
        RuntimeAttr? runtime_attr_override
    }

    scatter (vcf_shard in vcf_shards) {
        call countSNVsandIndels {
        input:
            input_vcf=vcf_shard,
            hail_docker=hail_docker,
            runtime_attr_override=runtime_attr_override
        }
    }

    call aggregateCounts {
        input:
            snv_counts=countSNVsandIndels.snv_count,
            indel_counts=countSNVsandIndels.indel_count
    }

    output {
        File total_snv_count = aggregateCounts.total_snv_count
        File total_indel_count = aggregateCounts.total_indel_count
    }
}

task countSNVsandIndels {
    input {
        File input_vcf
        String hail_docker
        RuntimeAttr? runtime_attr_override
    }
    Float input_size = size(input_vcf, 'GB')
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
        set -eou pipefail
        grep -v "^#" ~{input_vcf} | awk 'length($4) == 1 && length($5) == 1' | wc -l > snv_counts.txt
        grep -v "^#" ~{input_vcf} | awk 'length($4) != length($5)' | wc -l > indel_counts.txt
    >>>

    output {
        Int snv_count = read_int("snv_counts.txt")
        Int indel_count = read_int("indel_counts.txt")
    }
}

task aggregateCounts {
    input {
        Array[Int] snv_counts
        Array[Int] indel_counts
    }

    command <<<
        echo "$(awk '{s+=$1} END {print s}' <<< "~{sep=' ' snv_counts}")" > total_snv_count.txt
        echo "$(awk '{s+=$1} END {print s}' <<< "~{sep=' ' indel_counts}")" > total_indel_count.txt
    >>>

    output {
        File total_snv_count = "total_snv_counts.txt"
        File total_indel_count = "total_indel_counts.txt"
    }
}
