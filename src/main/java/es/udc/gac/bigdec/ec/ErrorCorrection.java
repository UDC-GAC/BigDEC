/*
 * Copyright (C) 2022 Universidade da Coruña
 *
 * This file is part of BigDEC.
 *
 * BigDEC is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * BigDEC is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with BigDEC. If not, see <http://www.gnu.org/licenses/>.
 */
package es.udc.gac.bigdec.ec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.udc.gac.bigdec.RunEC;
import es.udc.gac.bigdec.RunMerge;
import es.udc.gac.bigdec.RunEC.ExecutionEngine;
import es.udc.gac.bigdec.ec.musket.MUSKET;
import es.udc.gac.bigdec.ec.reckoner.RECKONER;
import es.udc.gac.bigdec.sequence.SequenceParserFactory.FileFormat;
import es.udc.gac.bigdec.util.CLIOptions;
import es.udc.gac.bigdec.util.Configuration;
import es.udc.gac.bigdec.util.IOUtils;
import es.udc.gac.bigdec.util.Timer;
import es.udc.gac.hadoop.sequence.parser.mapreduce.FastAInputFormat;
import es.udc.gac.hadoop.sequence.parser.mapreduce.FastQInputFormat;
import es.udc.gac.hadoop.sequence.parser.mapreduce.SingleEndSequenceInputFormat;

public abstract class ErrorCorrection {
	public static final int QS_HISTOGRAM_SIZE = 127;
	public static final int KMER_HISTOGRAM_SIZE = 256;
	public static final short KMER_MIN_COUNTER = 2;
	public static final short KMER_MAX_COUNTER = KMER_HISTOGRAM_SIZE - 1;

	private static final Logger logger = LoggerFactory.getLogger(ErrorCorrection.class);
	private static final String CREATE_DATASETS_TIME = "CREATE_DATASETS_TIME";
	private static final String CREATE_QS_HISTOGRAM_TIME = "CREATE_QS_HISTOGRAM_TIME";
	private static final String CREATE_KMER_HISTOGRAM_TIME = "CREATE_KMER_HISTOGRAM_TIME";
	private static final String KMER_COUNTING_TIME = "KMER_COUNTING_TIME";
	private static final String ANALYZE_HISTOGRAMS_TIME = "ANALYZE_HISTOGRAMS_TIME";
	private static final String FILTER_SOLID_KMERS_TIME = "FILTER_SOLID_KMERS_TIME";
	private static final String SAVE_SOLID_KMERS_TIME = "SAVE_SOLID_KMERS_TIME";
	private static final String MERGE_SOLID_KMERS_TIME = "MRGE_SOLID_KMERS_TIME";
	private static final String LOAD_SOLID_KMERS_TIME = "LOAD_SOLID_KMERS_TIME";
	private static final String ERROR_CORRECTION_TIME = "ERROR_CORRECTION_TIME";
	private static final String CLEANUP_TIME = "CLEANUP_TIME";
	private static final String MERGER_THREAD_TIME = "MERGER_THREAD_TIME";

	private Configuration config;
	private CLIOptions options;
	private FileSystem fs;
	private byte kmerLength;
	private int[] kmerHistogram;
	private FileFormat fileFormat;
	private boolean ignoreNBases;
	private boolean compressOutput;
	private String outputCodec;
	private int parallelism;
	private org.apache.hadoop.conf.Configuration hadoopConfig;
	private int[] qsHistogram;
	private boolean buildQsHistrogram;
	private List<CorrectionAlgorithm> correctionAlgorithms;
	private Map<String,MergerThread> mergerThreads;
	private Timer timer;
	private Path inputFile1;
	private Path inputFile2;
	private Path outputFile1;
	private Path outputFile2;
	private Path qsHistogramPath;
	private Path kmerHistogramPath;
	private Path kmersPath;
	private Path solidKmersPath;
	private Path solidKmersFile;
	private long nsplits;
	private long blockSize;
	private int sequenceSize;

	public ErrorCorrection(Configuration config, CLIOptions options) {
		this.config = config;
		this.options = options;
		fs = null;
		kmerLength = options.getKmerLength();
		parallelism = 1;
		nsplits = 1;
		hadoopConfig = null;
		correctionAlgorithms = config.getCorrectionAlgorithms(kmerLength);
		mergerThreads = null;
		timer = new Timer();
		inputFile1 = null;
		inputFile2 = null;
		outputFile1 = null;
		outputFile2 = null;
		qsHistogram = null;
		kmerHistogram = null;
		buildQsHistrogram = true;
		qsHistogramPath = null;
		kmersPath = null;
		kmerHistogramPath = null;
		solidKmersPath = null;
		solidKmersFile = null;
		outputCodec = null;
		sequenceSize = 0;

		if (correctionAlgorithms.size() == 1 && correctionAlgorithms.get(0) instanceof MUSKET)
			ignoreNBases = true;
		else
			ignoreNBases = false;
	}

	public Logger getLogger() {
		return logger;
	}

	public Configuration getConfig() {
		return config;
	}

	public CLIOptions getCLIOptions() {
		return options;
	}

	public byte getKmerLength() {
		return kmerLength;
	}

	public FileFormat getFileFormat() {
		return fileFormat;
	}

	public boolean isIgnoreNBases() {
		return ignoreNBases;
	}

	public Class<? extends SingleEndSequenceInputFormat> getInputFormatClass() {
		if (fileFormat == FileFormat.FILE_FORMAT_FASTQ)
			return FastQInputFormat.class;
		else
			return FastAInputFormat.class;
	}

	public boolean isPaired() {
		return options.isPaired();
	}

	public FileSystem getFileSystem() {
		return fs;
	}

	public int getParallelism() {
		return parallelism;
	}

	public void setParallelism(int parallelism) {
		this.parallelism = parallelism;
	}

	public org.apache.hadoop.conf.Configuration getHadoopConfig() {
		return hadoopConfig;
	}

	public void setHadoopConfig(org.apache.hadoop.conf.Configuration hadoopConfig) {
		this.hadoopConfig = hadoopConfig;
	}

	public int getNumberOfAlgorithms() {
		return correctionAlgorithms.size();
	}

	public boolean isCompressOutput() {
		return compressOutput;
	}

	public String getOutputCodec() {
		return outputCodec;
	}

	public List<CorrectionAlgorithm> getCorrectionAlgorithms() {
		return correctionAlgorithms;
	}

	public Path getInputFile1() {
		return inputFile1;
	}

	public Path getInputFile2() {
		return inputFile2;
	}

	public Path getOutputFile1() {
		return outputFile1;
	}

	public Path getOutputFile2() {
		return outputFile2;
	}

	public String getOutputDir() {
		return options.getOutputDir();
	}

	public Path getQsHistoPath() {
		return qsHistogramPath;
	}

	public Path getKmerHistoPath() {
		return kmerHistogramPath;
	}

	public Path getKmersPath() {
		return kmersPath;
	}

	public Path getSolidKmersPath() {
		return solidKmersPath;
	}

	public Path getSolidKmersFile() {
		return solidKmersFile;
	}

	public long getNumberOfSplits() {
		return nsplits;
	}

	public int getSequenceSize() {
		return sequenceSize;
	}

	public void setKmerHistogram(int[] histogram) {
		kmerHistogram = histogram;
	}

	public void setQsHistogram(int[] histogram) {
		qsHistogram = histogram;
	}

	public void putMergePath(Path path) {
		if (mergerThreads != null && config.MULTITHREAD_MERGE) {
			try {
				MergerThread thread = mergerThreads.get(path.toString());

				if (thread != null) {
					thread.putPath(path);
				} else {
					if (RunEC.EXECUTION_ENGINE == ExecutionEngine.FLINK_MODE && config.FLINK_PAIRED_MERGE)
						logger.warn("MergerThread for {} does not exist", path.toString());
				}
			} catch (InterruptedException e) {
				IOUtils.error(e.getMessage());
			}
		}
	}

	protected abstract void createDatasets() throws IOException;
	protected abstract int[] buildQsHistogram() throws IOException;
	protected abstract void kmerCounting(short minKmerCounter, short maxKmerCounter);
	protected abstract int[] buildKmerHistrogram() throws IOException;
	protected abstract void writeSolidKmersAsCSV(short kmerThreshold, short maxKmerCounter);
	protected abstract void loadSolidKmers(int numberOfSolidKmers);
	protected abstract void filterSolidKmers(short kmerThreshold);
	protected abstract void removeSolidKmers();
	protected abstract void runErrorCorrection(CorrectionAlgorithm algorithm);
	protected abstract void runErrorCorrection(List<CorrectionAlgorithm> correctionAlgorithms);
	protected abstract void destroyDatasets();

	public long getEstimatedPartitionSize() {
		return IOUtils.getSplitSize();
	}

	public void createHistograms(Path inputFile1, Path inputFile2, long nsplits) throws IOException {
		this.inputFile1 = inputFile1;
		this.inputFile2 = inputFile2;
		fs = FileSystem.newInstance(hadoopConfig);
		blockSize = fs.getFileStatus(inputFile1).getBlockSize();

		outputFile1 = new Path(options.getMergeOutputDir()+Configuration.SLASH+inputFile1.getName());

		if (options.isPaired())
			outputFile2 = new Path(options.getMergeOutputDir()+Configuration.SLASH+inputFile2.getName());

		qsHistogramPath = new Path(options.getOutputDir()+Configuration.SLASH+"histo.qs");
		kmerHistogramPath = new Path(options.getOutputDir()+Configuration.SLASH+"histo.kmer");
		kmersPath = new Path(options.getOutputDir()+Configuration.SLASH+"kmers");
		solidKmersPath = new Path(options.getOutputDir()+Configuration.SLASH+"solidkmers");
		solidKmersFile = new Path(options.getOutputDir()+Configuration.SLASH+"solidkmers.csv");

		if (fs.exists(kmersPath))
			fs.delete(kmersPath, true);

		if (fs.exists(solidKmersPath))
			fs.delete(solidKmersPath, true);

		if (fs.exists(solidKmersFile))
			fs.delete(solidKmersFile, true);

		if (fs.exists(qsHistogramPath))
			fs.delete(qsHistogramPath, true);

		if (fs.exists(kmerHistogramPath))
			fs.delete(kmerHistogramPath, true);

		/*
		 * Detect file input format and compression
		 */
		fileFormat = IOUtils.getInputFileFormat(hadoopConfig, inputFile1);
		compressOutput = IOUtils.isPathCompressed(hadoopConfig, inputFile1);
		sequenceSize = IOUtils.getFirstSequenceSize(fs, inputFile1);
		this.nsplits = nsplits;

		if (compressOutput) {
			outputCodec = IOUtils.getCompressionCodec(hadoopConfig, inputFile1).getClass().getCanonicalName();
			IOUtils.enableOutputCompression(outputCodec, hadoopConfig);
		}

		logger.info("nsplits = {}, fileFormat = {}, compressOutput = {}", nsplits, fileFormat, compressOutput);

		/*
		 * Create input datasets
		 */
		timer.start(CREATE_DATASETS_TIME);
		createDatasets();
		timer.stop(CREATE_DATASETS_TIME);

		/*
		 * Create and analyze histograms
		 */
		timer.start(CREATE_QS_HISTOGRAM_TIME);
		createQsHistogram();
		timer.stop(CREATE_QS_HISTOGRAM_TIME);
		timer.start(CREATE_KMER_HISTOGRAM_TIME);
		createKmerHistogram();
		timer.stop(CREATE_KMER_HISTOGRAM_TIME);
		timer.start(ANALYZE_HISTOGRAMS_TIME);
		analyzeHistograms();
		timer.stop(ANALYZE_HISTOGRAMS_TIME);
	}

	private void createQsHistogram() throws IOException {
		/*
		 * Quality score histogram is not built when using MUSKET or RECKONER
		 */
		if (correctionAlgorithms.size() == 1 && correctionAlgorithms.get(0) instanceof MUSKET)
			buildQsHistrogram = false;

		if (correctionAlgorithms.size() == 1 && correctionAlgorithms.get(0) instanceof RECKONER)
			buildQsHistrogram = false;

		if (correctionAlgorithms.size() == 2) {
			if (correctionAlgorithms.get(0) instanceof RECKONER && correctionAlgorithms.get(1) instanceof MUSKET)
				buildQsHistrogram = false;

			if (correctionAlgorithms.get(1) instanceof RECKONER && correctionAlgorithms.get(0) instanceof MUSKET)
				buildQsHistrogram = false;
		}

		/*
		 * Build quality score histogram
		 */
		if (buildQsHistrogram)
			qsHistogram = buildQsHistogram();
		else
			qsHistogram = null;
	}

	private void createKmerHistogram() throws IOException {
		short minKmerCounter = KMER_MIN_COUNTER;

		if (config.KMER_THRESHOLD != 0)
			minKmerCounter = config.KMER_THRESHOLD;

		logger.info("minKmerCounter = {}, ignoreNBases = {}", minKmerCounter, ignoreNBases);

		/*
		 * Count k-mers
		 */
		timer.start(KMER_COUNTING_TIME);
		kmerCounting(minKmerCounter, KMER_MAX_COUNTER);
		timer.stop(KMER_COUNTING_TIME);

		/*
		 * Build k-mer histogram
		 */		
		kmerHistogram = buildKmerHistrogram();
	}

	private void analyzeHistograms() throws IOException {
		List<CorrectionAlgorithm> remove = new ArrayList<CorrectionAlgorithm>();

		if (RunEC.EXECUTION_ENGINE == ExecutionEngine.FLINK_MODE) {
			if (buildQsHistrogram && qsHistogram == null)
				qsHistogram = IOUtils.loadHistogram(fs, qsHistogramPath, QS_HISTOGRAM_SIZE, false);

			if (kmerHistogram == null)
				kmerHistogram = IOUtils.loadHistogram(fs, kmerHistogramPath, KMER_HISTOGRAM_SIZE, false);
		}

		/*
		 * Analyze quality score histogram/reads to determine the quality score offset/threshold
		 */
		for (CorrectionAlgorithm algorithm: correctionAlgorithms) {
			try {
				algorithm.determineQsOffset(fs, inputFile1);
				if (isPaired())
					algorithm.determineQsOffset(fs, inputFile2);

				algorithm.determineQsCutoff(qsHistogram);
			} catch (CorrectionAlgorithmException e) {
				IOUtils.warn(e.getMessage());
				IOUtils.warn("Disabling correction algorithm "+algorithm.toString());
				remove.add(algorithm);
			}
		}

		if(!remove.isEmpty())
			correctionAlgorithms.removeAll(remove);

		/*
		 * Write the quality score histogram to a file
		 */
		if (qsHistogram != null)
			IOUtils.writeHistogram(fs, qsHistogramPath, qsHistogram, 0, QS_HISTOGRAM_SIZE);

		if(correctionAlgorithms.isEmpty())
			IOUtils.error("No correction algorithms available to continue");

		/*
		 * Analyze the k-mer histogram to determine the k-mer occurrence threshold
		 * (if not set by the user)
		 */
		remove.clear();
		for (CorrectionAlgorithm algorithm: correctionAlgorithms) {
			if (config.KMER_THRESHOLD != 0) {
				algorithm.setKmerThreshold(config.KMER_THRESHOLD);
			} else {
				try {
					IOUtils.info(algorithm.toString()+": analyzing k-mer histogram");
					algorithm.determineKmerCutoff(kmerHistogram);

					/*
					 * Count the number of unique solid k-mers
					 */
					int uniqueSolidKmers = 0;
					for (int i = algorithm.getKmerThreshold(); i < ErrorCorrection.KMER_HISTOGRAM_SIZE; i++)
						uniqueSolidKmers += kmerHistogram[i];

					IOUtils.info(" -k-mer threshold = "+algorithm.getKmerThreshold());
					IOUtils.info(" -number of unique solid k-mers = "+uniqueSolidKmers);
					algorithm.setNumberOfSolidKmers(uniqueSolidKmers);
				} catch (CorrectionAlgorithmException e) {
					IOUtils.warn(e.getMessage());
					IOUtils.warn("disabling correction algorithm "+algorithm.toString());
					remove.add(algorithm);
					continue;
				}
			}
		}

		if(!remove.isEmpty())
			correctionAlgorithms.removeAll(remove);

		/*
		 * Write the k-mer histogram to a file
		 */
		long nkmers = IOUtils.writeHistogram(fs, kmerHistogramPath, kmerHistogram, 0, KMER_HISTOGRAM_SIZE);

		if (config.KMER_THRESHOLD != 0) {
			IOUtils.info("k-mer threshold = "+config.KMER_THRESHOLD);
			IOUtils.info("number of unique solid k-mers = "+nkmers);
		} else {
			IOUtils.info("number of counted k-mers = "+nkmers);
		}

		if(correctionAlgorithms.isEmpty())
			IOUtils.error("No correction algorithms available to continue");
	}

	public List<Path> correctErrors(Path outputPath1, Path outputPath2) throws IOException {
		List<Path> outputPaths = new ArrayList<Path>();
		int previousThreshold = Integer.MAX_VALUE;
		Path tmp;

		// Sort algorithms in ascending order according to their k-mer threshold
		correctionAlgorithms.sort(Comparator.comparing(CorrectionAlgorithm::getKmerThreshold));
		CorrectionAlgorithm firstAlgorithm = correctionAlgorithms.get(0);

		logger.debug("minimum k-mer threshold = {}", firstAlgorithm.getKmerThreshold());

		// Build list of output paths
		for (CorrectionAlgorithm algorithm: correctionAlgorithms) {
			tmp = new Path(outputPath1+Configuration.SLASH+algorithm.toString());
			outputPaths.add(tmp);
			algorithm.setOutputPath1(tmp.toString());

			if (isPaired()) {
				tmp = new Path(outputPath2+Configuration.SLASH+algorithm.toString());
				outputPaths.add(tmp);
				algorithm.setOutputPath2(tmp.toString());
			}
		}

		IOUtils.info("number of algorithms = "+getNumberOfAlgorithms());
		IOUtils.info("execution order:");
		for (CorrectionAlgorithm algorithm: correctionAlgorithms)
			IOUtils.info("  "+algorithm.toString()+" (threshold = "+algorithm.getKmerThreshold()+")");

		/*
		 * Filter k-mers using the minimum k-mer threshold value to obtain solid ones
		 * and write them to a file in CSV format
		 */
		IOUtils.info("filtering solid k-mers (threshold = "+firstAlgorithm.getKmerThreshold()+")");
		IOUtils.info("writing solid k-mers to "+getSolidKmersFile());

		timer.start(SAVE_SOLID_KMERS_TIME);
		writeSolidKmersAsCSV(firstAlgorithm.getKmerThreshold(), KMER_MAX_COUNTER);

		// Merge k-mer files
		try {
			timer.start(MERGE_SOLID_KMERS_TIME);

			if (RunEC.EXECUTION_ENGINE == ExecutionEngine.FLINK_MODE && getParallelism() == 1) {
				fs.rename(getSolidKmersPath(), getSolidKmersFile());
			} else {
				List<Path> inputFiles = RunMerge.getFiles(fs, getSolidKmersPath(), false);
				RunMerge.merge(fs, getSolidKmersPath(), inputFiles, fs, getSolidKmersFile(), 3, true, getHadoopConfig());
			}

			timer.stop(MERGE_SOLID_KMERS_TIME);

			if (RunEC.EXECUTION_ENGINE == ExecutionEngine.FLINK_MODE) {
				if (config.FLINK_WRITE_KMERS && config.HDFS_DELETE_TEMP_FILES) {
					if (fs.exists(kmersPath))
						fs.delete(kmersPath, true);
				}
			}

			long fileSize = fs.getFileStatus(getSolidKmersFile()).getLen();
			double fileSizeMB = IOUtils.ByteToMiB(fileSize);

			if (fileSizeMB > 1)
				IOUtils.info("solid k-mers file size: "+IOUtils.formatOneDecimal(fileSizeMB)+" MiBytes");
			else
				IOUtils.info("solid k-mers file size: "+fileSize+" bytes");
		} catch (IOException e) {
			IOUtils.error(e.getMessage());
		}
		timer.stop(SAVE_SOLID_KMERS_TIME);

		/*
		 * Start merger threads
		 */
		if (options.runMergerThread())
			mergerThreads = runMergerThreads(outputPaths);

		/*
		 * Correct errors in reads
		 */
		if (RunEC.EXECUTION_ENGINE == ExecutionEngine.FLINK_MODE) {
			if (!config.FLINK_MULTIPLE_JOB) {
				timer.start(ERROR_CORRECTION_TIME);
				runErrorCorrection(correctionAlgorithms);
				timer.stop(ERROR_CORRECTION_TIME);
			} else {
				for (CorrectionAlgorithm algorithm: correctionAlgorithms) {
					IOUtils.info("executing algorithm "+algorithm.toString());
					algorithm.printConfig();
					timer.start(ERROR_CORRECTION_TIME);
					timer.start(algorithm.getClass().getName());
					runErrorCorrection(algorithm);
					timer.stop(algorithm.getClass().getName());
					timer.stop(ERROR_CORRECTION_TIME);
				}
			}
		} else {
			/*
			 * Load solid k-mers to make them available on workers
			 */
			timer.start(LOAD_SOLID_KMERS_TIME);
			loadSolidKmers(firstAlgorithm.getNumberOfSolidKmers());
			timer.stop(LOAD_SOLID_KMERS_TIME);

			/*
			 *  For each correction algorithm
			 */
			for (CorrectionAlgorithm algorithm: correctionAlgorithms) {
				IOUtils.info("executing algorithm "+algorithm.toString());

				/*
				 * Filter solid k-mers
				 */
				if (algorithm.getKmerThreshold() > previousThreshold) {
					timer.start(FILTER_SOLID_KMERS_TIME);
					IOUtils.info("filtering solid k-mers (threshold = "+algorithm.getKmerThreshold()+")");
					filterSolidKmers(algorithm.getKmerThreshold());
					timer.stop(FILTER_SOLID_KMERS_TIME);
				}

				previousThreshold = algorithm.getKmerThreshold();
				algorithm.printConfig();
				timer.start(ERROR_CORRECTION_TIME);
				timer.start(algorithm.getClass().getName());
				runErrorCorrection(algorithm);
				timer.stop(algorithm.getClass().getName());
				timer.stop(ERROR_CORRECTION_TIME);
			}
		}

		if (options.runMergerThread() && mergerThreads != null) {
			for (MergerThread thread: mergerThreads.values()) {
				if (thread.isAlive()) {
					timer.start(MERGER_THREAD_TIME);
					try {
						IOUtils.info("waiting for merger thread...");
						thread.join();
					} catch (InterruptedException e) {
						thread.terminate();
						IOUtils.error("InterruptedException waiting for merger thread: "+e.getMessage());
					}
					timer.stop(MERGER_THREAD_TIME);
				}
			}

			timer.start(CLEANUP_TIME);
			if (config.HDFS_DELETE_TEMP_FILES) {
				for (Path out: outputPaths) {
					fs.delete(out, true);
				}
			}
			timer.stop(CLEANUP_TIME);
		}

		/*
		 * Free resources
		 */
		timer.start(CLEANUP_TIME);
		removeSolidKmers();
		destroyDatasets();
		timer.stop(CLEANUP_TIME);

		return outputPaths;
	}

	public void printECTimes() {
		double total = timer.getTotalTime(CREATE_DATASETS_TIME) +
				timer.getTotalTime(CREATE_QS_HISTOGRAM_TIME) +
				timer.getTotalTime(KMER_COUNTING_TIME) +
				timer.getTotalTime(CREATE_KMER_HISTOGRAM_TIME) +
				timer.getTotalTime(ANALYZE_HISTOGRAMS_TIME) +
				timer.getTotalTime(FILTER_SOLID_KMERS_TIME) +
				timer.getTotalTime(SAVE_SOLID_KMERS_TIME) +
				timer.getTotalTime(LOAD_SOLID_KMERS_TIME) +
				timer.getTotalTime(ERROR_CORRECTION_TIME) +
				timer.getTotalTime(MERGER_THREAD_TIME) +
				timer.getTotalTime(CLEANUP_TIME);

		IOUtils.info("########### EC TIMES #############");
		IOUtils.info("total: "+IOUtils.formatTwoDecimal(total)+" seconds");
		IOUtils.info(" -datasets creation = " +IOUtils.formatTwoDecimal(timer.getTotalTime(CREATE_DATASETS_TIME))+" seconds");
		IOUtils.info(" -quality score histogram = " +IOUtils.formatTwoDecimal(timer.getTotalTime(CREATE_QS_HISTOGRAM_TIME))+" seconds");
		IOUtils.info(" -k-mer counting = " +IOUtils.formatTwoDecimal(timer.getTotalTime(KMER_COUNTING_TIME))+" seconds");
		IOUtils.info(" -k-mer histogram = " +IOUtils.formatTwoDecimal(timer.getTotalTime(CREATE_KMER_HISTOGRAM_TIME))+" seconds");
		IOUtils.info(" -analyze histograms = " +IOUtils.formatTwoDecimal(timer.getTotalTime(ANALYZE_HISTOGRAMS_TIME))+" seconds");
		if (timer.getTotalTime(SAVE_SOLID_KMERS_TIME) != 0)
			IOUtils.info(" -save solid k-mers = " +IOUtils.formatTwoDecimal(timer.getTotalTime(SAVE_SOLID_KMERS_TIME))+" seconds");
		if (timer.getTotalTime(MERGE_SOLID_KMERS_TIME) != 0)
			IOUtils.info(" -merge solid k-mers = " +IOUtils.formatTwoDecimal(timer.getTotalTime(MERGE_SOLID_KMERS_TIME))+" seconds");		
		if (timer.getTotalTime(LOAD_SOLID_KMERS_TIME) != 0)
			IOUtils.info(" -load solid k-mers = " +IOUtils.formatTwoDecimal(timer.getTotalTime(LOAD_SOLID_KMERS_TIME))+" seconds");
		if (timer.getTotalTime(FILTER_SOLID_KMERS_TIME) != 0)
			IOUtils.info(" -filter solid k-mers = " +IOUtils.formatTwoDecimal(timer.getTotalTime(FILTER_SOLID_KMERS_TIME))+" seconds");
		IOUtils.info(" -read correction = " +IOUtils.formatTwoDecimal(timer.getTotalTime(ERROR_CORRECTION_TIME))+" seconds");

		for (CorrectionAlgorithm alg: correctionAlgorithms) {
			if (timer.getTotalTime(alg.getClass().getName()) != 0)
				IOUtils.info("    "+alg.getClass().getSimpleName()+" = " +IOUtils.formatTwoDecimal(timer.getTotalTime(alg.getClass().getName()))+" seconds");
		}

		if (timer.getTotalTime(CLEANUP_TIME) != 0)
			IOUtils.info(" -cleanup = " +IOUtils.formatTwoDecimal(timer.getTotalTime(CLEANUP_TIME))+" seconds");
		if (options.runMergerThread() && timer.getTotalTime(MERGER_THREAD_TIME) != 0)
			IOUtils.info(" -merger thread = " +IOUtils.formatTwoDecimal(timer.getTotalTime(MERGER_THREAD_TIME))+" seconds");
		IOUtils.info("##################################");
	}

	public void mergeOutput(Path outputPath, Path outputFile) throws IOException {
		FileSystem srcFS = FileSystem.newInstance(hadoopConfig);
		Path mergeOutputPath = new Path(options.getMergeOutputDir());
		FileSystem dstFS = mergeOutputPath.getFileSystem(hadoopConfig);

		// Get sorted list of files to merge
		List<Path> inputFiles = RunMerge.getFiles(srcFS, outputPath, true);

		if (logger.isDebugEnabled()) {
			logger.debug("Files to merge");
			for (Path file: inputFiles) {
				logger.debug(file.toString());
			}
		}

		// Merge files
		RunMerge.merge(srcFS, outputPath, inputFiles, dstFS, outputFile,
				config.HDFS_BLOCK_REPLICATION, blockSize, config.HDFS_DELETE_TEMP_FILES, hadoopConfig);
	}

	private Map<String,MergerThread> runMergerThreads(List<Path> outputPaths) throws IOException {
		int timeout = (hadoopConfig.getInt("dfs.client.socket-timeout", 120000)) * 2;
		hadoopConfig.setInt("ipc.ping.interval", timeout);
		hadoopConfig.setInt("ipc.client.rpc-timeout.ms", timeout);
		hadoopConfig.setInt("dfs.client.socket-timeout", timeout);

		Path mergeOutputPath = new Path(options.getMergeOutputDir());
		FileSystem dstFS = mergeOutputPath.getFileSystem(hadoopConfig);
		Map<String,MergerThread> mergerThreads = new HashMap<String,MergerThread>(outputPaths.size());
		Path done = new Path("done");
		long outputFiles;
		long fileSize = getEstimatedPartitionSize() - sequenceSize;

		if (RunEC.EXECUTION_ENGINE == ExecutionEngine.SPARK_MODE)
			outputFiles = nsplits;
		else
			outputFiles = getParallelism();

		if (!config.MULTITHREAD_MERGE) {
			FileSystem srcFS = FileSystem.newInstance(hadoopConfig);

			MergerThread mergerThread = new MergerThread(srcFS, dstFS, outputPaths, null,
					outputFile1, outputFile2, outputFiles, done, true, fileSize, blockSize, config, hadoopConfig);

			mergerThreads.put("ALL", mergerThread);
		} else {
			MergerThread mergerThread;
			List<Path> paths;
			FileSystem srcFS;

			for (CorrectionAlgorithm algorithm: correctionAlgorithms) {
				srcFS = FileSystem.newInstance(hadoopConfig);
				paths = new ArrayList<Path>();
				paths.add(new Path(algorithm.getOutputPath1().toString()));
				BlockingQueue<Path> queue = new ArrayBlockingQueue<Path>(1);

				mergerThread = new MergerThread(srcFS, dstFS, paths, queue,
						outputFile1, outputFile2, outputFiles, done, true, fileSize, blockSize, config, hadoopConfig);

				mergerThreads.put(algorithm.getOutputPath1().toString(), mergerThread);

				if (isPaired()) {
					if (RunEC.EXECUTION_ENGINE == ExecutionEngine.SPARK_MODE || config.FLINK_PAIRED_MERGE) {
						srcFS = FileSystem.newInstance(hadoopConfig);
						paths = new ArrayList<Path>();
						paths.add(new Path(algorithm.getOutputPath2().toString()));
						BlockingQueue<Path> pairedQueue = new ArrayBlockingQueue<Path>(1);

						mergerThread = new MergerThread(srcFS, dstFS, paths, pairedQueue,
								outputFile1, outputFile2, outputFiles, done, true, fileSize, blockSize, config, hadoopConfig);

						mergerThreads.put(algorithm.getOutputPath2().toString(), mergerThread);
					} else {
						mergerThread.getPaths().add(new Path(algorithm.getOutputPath2().toString()));
					}
				}
			}
		}

		for (MergerThread thread: mergerThreads.values())
			thread.start();

		logger.info("outputFiles {}, partitionSize {}, sequenceSize {}, fileSize {}, timeout {} ms", outputFiles, 
				getEstimatedPartitionSize(), sequenceSize, fileSize, timeout);

		return mergerThreads;
	}
}
