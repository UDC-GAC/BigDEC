# BigDEC: a multi-algorithm Big Data tool for scalable short-read error correction

**BigDEC** is a parallel error corrector intended for short DNA reads that is built upon two popular open-source Big Data frameworks: [Apache Spark](https://spark.apache.org) and [Apache Flink](https://flink.apache.org). This tool integrates three different correction algorithms based on the k-mer spectrum method: [Musket](http://musket.sourceforge.net/homepage.htm), [BLESS 2](https://sourceforge.net/projects/bless-ec) and [RECKONER](https://github.com/refresh-bio/RECKONER).

This tool supports the processing of single-end and paired-end reads from FASTQ datasets stored in the [Hadoop Distributed File System (HDFS)](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html).

### Citation

If you use **BigDEC** in your research, please cite our work using the following reference:

> Roberto R. Expósito, Jorge González-Domínguez. [BigDEC: A multi-algorithm Big Data tool based on the k-mer spectrum method for scalable short-read error correction](https://doi.org/10.1016/j.future.2024.01.011). Future Generation Computer Systems 154: 314-329 (2024).

## Getting Started

### Prerequisites

* Make sure you have Java Runtime Environment (JRE) version 1.8 or above.
  * JAVA_HOME environmental variable must be set accordingly.

* For Spark, make sure you have a working distribution version 2.4 or above.
  * Note that the *spark-submit* command needs to be available in PATH.
  * See [Spark's Cluster Overview](https://spark.apache.org/docs/latest/cluster-overview.html).

* For Flink, make sure you have a working distribution version 1.12 or above.
  * Note that the *flink* command needs to be available in PATH.
  * See [Flink's Cluster Overview](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/concepts/flink-architecture/#anatomy-of-a-flink-cluster).

* Download BigDEC from releases page and unzip the tarball. Alternatively, clone the github repository by executing the following command:

```
git clone https://github.com/UDC-GAC/BigDEC.git
```

* Set BIGDEC_HOME and PATH environmental variables. On Linux, you can set them in your profile or your shell configuration files (e.g., .bashrc). Follow the instructions below:

```
export BIGDEC_HOME=/path/to/bigdec
export PATH=$BIGDEC_HOME/bin:$PATH
```

### Execution

BigDEC can be executed by using the provided *sparkrun* or *flinkrun* commands, which launches the Spark/Flink jobs to the cluster. Their basic usage is as follows:

```
sparkrun|flinkrun --args "BIGDEC_ARGS" [SPARK_ARGS|FLINK_ARGS]
```

These commands accepts any argument to be passed to BigDEC by using --args "BIGDEC_ARGS" (e.g. --args "-s /path/to/dataset.fastq -k 24"), while the rest of the specified parameters are forwarded to the Spark or Flink runtimes. The command-line arguments available for BigDEC are:

* **-s \<file>**. Compulsory in single-end scenarios. String with the HDFS path to the input sequence file in FASTQ format.
* **-p \<file1> \<file2>**. Compulsory in paired-end scenarios. Two strings with the HDFS paths to the forward and reverse input sequence files in FASTQ format.
* **-o \<dir>**. Output directory for storing all the corrected files and other stuff required by the tool.
* **-c \<file>**. Path to the BigDEC configuration file for advanced settings about the correction algorithms, Spark, Flink and HDFS, among others.
* **-k \<int>**. k-mer length used for correction. The default value is 21.
* **-sc \<int>**. Number of input splits per CPU core. The default value is 8.
* **-m [\<dir>]**. Merge output files into a single one. The output directory is optional (use *file:/scheme* to specify a local file system as destination instead of HDFS).
* **-mtoff**. Turn off merger thread.
* **-h**. Print out the usage of the tool and exit.
* **-v**. Print out the version of the tool and exit.

### Examples

The following command corrects a single-end dataset using Spark and specifies the output directory in HDFS:

```
sparkrun --args "-s dataset.fastq -o /output"
```

The following command corrects a paired-end dataset using Flink and k-mer length of 15, while merging the output files in HDFS:

```
flinkrun --args "-p dataset1.fastq dataset2.fastq -k 15 -m /output"
```

The following command corrects a paired-end dataset using Flink and k-mer length of 24, while merging the output files from HDFS to the local file system:

```
flinkrun --args "-p dataset1.fastq dataset2.fastq -k 24 -m file:/output"
```

The following command corrects a single-end compressed dataset using Spark and 16 input splits per core, while passing extra arguments to Spark runtime:

```
sparkrun --args "-s dataset.fastq.bz2 -sc 16" --master spark://207.184.161.138:7077 --deploy-mode client
```

The following command corrects a single-end dataset using Spark and specifies the path to a specific configuration file to be used:

```
sparkrun --args "-s dataset.fastq -c /path/to/config.properties"
```

The following command corrects a single-end dataset using Flink and 4 input splits per core, while passing extra arguments to Flink runtime:

```
flinkrun --args "-s dataset.fastq -sc 4" -p 32
```

To specify which algorithm(s) to use for correcting the input files, along with their parameters, see the next section.

## Configuration

As mentioned before, additional BigDEC settings can be set through the *config.properties* file located at the *conf* directory. The most relevant options that can be configured in this file are the following:

* **ALGORITHMS (list)**. Comma-separated list of correction algorithms to be used. Supported values: MUSKET, BLESS2, RECKONER, ALL. The last value can be used to specify all the algorithms currently supported. The default value is MUSKET.
* **KMER_THRESHOLD (int)**. Minimum occurrences for solid k-mers. This value is automatically calculated when set to 0, which is the default value.
* **KEEP_ORDER (boolean)**. Whether to force to keep output sequences in the same input order. The default value is true.
* **MULTITHREAD_MERGE (boolean)**. Whether to launch multiple threads when merging output files to improve performance. This setting is only relevant when multiple correction algorithms are executed and/or when correcting paired-end datasets. The default value is true.
* **HDFS_DELETE_TEMP_FILES (boolean)**. Delete the intermediate files created by BigDEC on HDFS (if any). The default value is false.
* **HDFS_BLOCK_REPLICATION (int)**. HDFS block replication factor for output files. The default value is 1.

In addition to the above parameters and a few others that are specific for Spark and Flink, most of the remaining parameters in this file allow to configure advanced settings that are specific to each correction algorithm, and they are intended to be modified only for expert users.

## Compilation

In case you need to recompile BigDEC the prerequisites are:

* Make sure you have Java Develpment Kit (JDK) version 1.8 or above.

* Make sure you have a working Apache Maven distribution version 3.5 or above.
  * See [Installing Apache Maven](https://maven.apache.org/install.html).

In order to build BigDEC just execute the following Maven command from within the BigDEC root directory:

```
mvn package
```

This will recreate the *jar* file needed to run BigDEC. Note that the first time you execute the previous command, Maven will download all the plugins and related dependencies it needs to fulfill the command. From a clean installation of Maven, this can take quite a while. If you execute the command again, Maven will now have what it needs, so it will be able to execute the command much more quickly.

## Authors

BigDEC is developed in the [Computer Architecture Group](https://gac.udc.es/?page_id=770&lang=en) at the [Universidade da Coruña](https://www.udc.es/en) by:

* **Roberto R. Expósito** (https://gac.udc.es/~rober)
* **Jorge González-Domínguez** (https://gac.udc.es/~jgonzalezd)

## License

This tool is distributed as free software and is publicly available under the GPLv3 license (see the [LICENSE](LICENSE) file for more details)
