version 1.0

struct RuntimeAttr {
    Float? mem_gb
    Int? cpu_cores
    Int? disk_gb
    Int? boot_disk_gb
    Int? preemptible_tries
    Int? max_retries
}

workflow addDPtoVCF {
    input {
        File input_vcf
        String add_dp_python_script
        String hail_docker
        RuntimeAttr? runtime_attr_override
    }

    call addDP {
        input:
            input_vcf=input_vcf,
            add_dp_python_script=add_dp_python_script,
            hail_docker=hail_docker,
            runtime_attr_override=runtime_attr_override
    }

    output {
        File output_vcf = addDP.output_vcf
        File output_vcf_idx = addDP.output_vcf_idx
    }
}

task addDP {
    input {
        File input_vcf
        String add_dp_python_script
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

    String output_filename = sub(basename(input_vcf), ".vcf", "") + ".DP.vcf"

    command <<<
        set -eou pipefail
        curl ~{add_dp_python_script} > add_dp.py
        python3.9 add_dp.py ~{input_vcf} ~{output_filename} ~{cpu_cores} ~{memory}
        cp $(ls . | grep hail*.log) hail_log.txt
    >>>

    output {
        File output_vcf = output_filename
        File output_vcf_idx = output_filename + '.tbi'
        File hail_log = "hail_log.txt"
    }
}