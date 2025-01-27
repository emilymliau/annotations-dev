version 1.0

struct RuntimeAttr {
    Float? mem_gb
    Int? cpu_cores
    Int? disk_gb
    Int? boot_disk_gb
    Int? preemptible_tries
    Int? max_retries
}

workflow addFiltertoVCF {
  input {
    Array[File] vcf_shards
    File missing_filters
    String sv_base_mini_docker
  }

  scatter (shard in vcf_shards) {
    call AddFilterToVCFShard {
      input:
        vcf_shard = shard,
        missing_filters = missing_filters,
        sv_base_mini_docker=sv_base_mini_docker
    }
  }

  output {
    Array[File] vcfs_with_filter = AddFilterToVCFShard.updated_vcf
    Array[File] vcfs_with_filter_idx = AddFilterToVCFShard.updated_vcf_idx
  }
}

task AddFilterToVCFShard {
    input {
        File vcf_shard
        File missing_filters
        String sv_base_mini_docker
        RuntimeAttr? runtime_attr_override
    }

    Float input_size = size(vcf_shard, 'GB')
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

    runtime {
        memory: "~{select_first([runtime_override.mem_gb, runtime_default.mem_gb])} GB"
        disks: "local-disk ~{select_first([runtime_override.disk_gb])} HDD"
        cpu: select_first([runtime_override.cpu_cores, runtime_default.cpu_cores])
        preemptible: select_first([runtime_override.preemptible_tries, runtime_default.preemptible_tries])
        maxRetries: select_first([runtime_override.max_retries, runtime_default.max_retries])
        docker: sv_base_mini_docker
        bootDiskSizeGb: select_first([runtime_override.boot_disk_gb, runtime_default.boot_disk_gb])
    }

    command <<<
        output_vcf="${vcf_shard%.vcf.bgz}.reheader.vcf.bgz"
        bcftools annotate -h ~{missing_filters} -Oz -o $output_vcf ~{vcf_shard}
        tabix $output_vcf
    >>>

    output {
        File updated_vcf = "${vcf_shard%.vcf.bgz}.reheader.vcf.bgz"
        File updated_vcf_idx = "${vcf_shard%.vcf.bgz}.reheader.vcf.bgz.tbi"
    }
}