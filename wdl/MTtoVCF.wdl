version 1.0

struct RuntimeAttr {
    Float? mem_gb
    Int? cpu_cores
    Int? disk_gb
    Int? boot_disk_gb
    Int? preemptible_tries
    Int? max_retries
}

workflow MTtoVCF {

    input {
        String mt_uri
        String bucket_id
        String hail_docker
        RuntimeAttr? runtime_attr_convert_mt_to_vcf
    }

    call convertMTtoVCF {
        input:
            mt_uri = mt_uri,
            bucket_id = bucket_id,
            hail_docker = hail_docker,
            runtime_attr_override = runtime_attr_convert_mt_to_vcf
    }

    output {
        String vcf_uri = convertMTtoVCF.vcf_uri
    }
}

task convertMTtoVCF {
    input {
        String mt_uri
        String bucket_id
        String hail_docker
        RuntimeAttr? runtime_attr_override
    }

    Float base_disk_gb = 10.0
    RuntimeAttr runtime_default = object {
        mem_gb: 8,
        disk_gb: base_disk_gb,
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
        set -euo pipefail

        python3.9 mt_to_vcf.py ~{mt_uri} ~{bucket_id} ~{cpu_cores} ~{memory}
    >>>

    output {
        String vcf_uri = read_lines('vcf_uri.txt')[0]
    }
}
