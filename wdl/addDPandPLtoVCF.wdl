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
        Array[File] vcf_files
        String add_dp_and_pl_python_script
        String hail_docker
        String genome_build='GRCh38'
        RuntimeAttr? runtime_attr_override
    }

    scatter (vcf_file in vcf_files) {
        call addDPandPL {
        input:
            vcf_file=vcf_file,
            add_dp_and_pl_python_script=add_dp_and_pl_python_script,
            hail_docker=hail_docker,
            genome_build=genome_build,
            runtime_attr_override=runtime_attr_override
        }
    }

    output {
        Array[File] reannotated_vcf_files = addDPandPL.reannotated_vcf
        Array[File] reannotated_vcf_indices = addDPandPL.reannotated_vcf_index
    }
}

task addDPandPL {
    input {
        File vcf_file
        String add_dp_and_pl_python_script
        String hail_docker
        String genome_build
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

    String base = basename(vcf_file)

    String output_filename =
    if base == sub(base, ".g.vcf.gz$", "") + ".g.vcf.gz" then sub(base, ".g.vcf.gz$", ".DP.PL.g.vcf.bgz")
    else if base == sub(base, ".gvcf.gz$", "") + ".gvcf.gz" then sub(base, ".gvcf.gz$", ".DP.PL.gvcf.bgz")
    else if base == sub(base, ".vcf.gz$", "") + ".vcf.gz" then sub(base, ".vcf.gz$", ".DP.PL.vcf.bgz")
    else if base == sub(base, ".g.vcf.bgz$", "") + ".g.vcf.bgz" then sub(base, ".g.vcf.bgz$", ".DP.PL.g.vcf.bgz")
    else if base == sub(base, ".gvcf.bgz$", "") + ".gvcf.bgz" then sub(base, ".gvcf.bgz$", ".DP.PL.gvcf.bgz")
    else if base == sub(base, ".vcf.bgz$", "") + ".vcf.bgz" then sub(base, ".vcf.bgz$", ".DP.PL.vcf.bgz")
    else if base == sub(base, ".g.vcf$", "") + ".g.vcf" then sub(base, ".g.vcf$", ".DP.PL.g.vcf.bgz")
    else if base == sub(base, ".gvcf$", "") + ".gvcf" then sub(base, ".gvcf$", ".DP.PL.gvcf.bgz")
    else if base == sub(base, ".vcf$", "") + ".vcf" then sub(base, ".vcf$", ".DP.PL.vcf.bgz")
    else base + ".DP.PL.bgz"

    command <<<
        set -eou pipefail
        curl ~{add_dp_and_pl_python_script} > add_dp_and_pl.py
        python3 add_dp_and_pl.py ~{vcf_file} ~{output_filename} ~{cpu_cores} ~{memory} ~{genome_build}
        # bgzip -c ~{output_filename} > ~{output_filename}.bgz
        cp $(ls . | grep hail*.log) hail_log.txt
    >>>

    output {
        File reannotated_vcf = "~{output_filename}"
        File reannotated_vcf_index = "~{output_filename}.tbi"
        File hail_log = "hail_log.txt"
    }
}