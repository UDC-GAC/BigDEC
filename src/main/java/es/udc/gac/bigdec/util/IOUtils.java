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
package es.udc.gac.bigdec.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.udc.gac.bigdec.RunEC;
import es.udc.gac.bigdec.sequence.SequenceParserFactory;
import es.udc.gac.bigdec.sequence.SequenceParserFactory.FileFormat;
import es.udc.gac.hadoop.sequence.parser.mapreduce.SingleEndSequenceInputFormat;

public final class IOUtils {
	private static final Logger logger = LoggerFactory.getLogger(IOUtils.class);
	private static final DecimalFormat ONE_DECIMAL_FORMAT = new DecimalFormat("0.0");
	private static final DecimalFormat TWO_DECIMAL_FORMAT = new DecimalFormat("0.00");
	private static final int ONE_MiB = 1024 * 1024;
	private static final String APP_NAME_TAG = "["+RunEC.APP_NAME+"] ";
	private static final String WARN_TAG = "[WARN] ";
	private static final String ERROR_TAG = "[ERROR] ";
	private static final String NEW_LINE = "\n";
	private static final String SEP_CHAR = ":";
	private static Path OUTPUT_PATH1;
	private static Path OUTPUT_PATH2;
	private static long splitSize;

	private IOUtils() {}

	public static Path getOutputPath1() {
		return OUTPUT_PATH1;
	}

	public static Path getOutputPath2() {
		return OUTPUT_PATH2;
	}

	public static long getSplitSize() {
		return splitSize;
	}

	public static long divideRoundUp(long num, long divisor) {
		return (num + divisor - 1) / divisor;
	}

	public static double ByteToMiB(long size) {
		return size / ((double)ONE_MiB);
	}

	public static String formatOneDecimal(double number) {
		return ONE_DECIMAL_FORMAT.format(number);
	}

	public static String formatTwoDecimal(double number) {
		return TWO_DECIMAL_FORMAT.format(number);
	}

	public static void info(String msg) {
		System.out.println(APP_NAME_TAG + msg);
	}

	public static void warn(String msg) {
		System.out.println(APP_NAME_TAG + WARN_TAG + msg);
	}

	public static void error(String msg) {
		System.out.println(APP_NAME_TAG + ERROR_TAG + msg);
		System.exit(-1);
	}

	public static long getNumberOfSplits(org.apache.hadoop.conf.Configuration hadoopConfig, CLIOptions options, Configuration config, 
			int parallelism) throws IOException {
		Path inputPath1 = null, inputPath2 = null;
		Path outputPath = null, mergeOutputPath = null;
		Path basePath;
		long inputPath1Length, inputPath2Length = 0;
		double inputPath1LengthMB, inputPath2LengthMB = 0;
		boolean isInputPath1Compressed, isInputPath2Compressed;
		boolean isInputPath1Splitable, isInputPath2Splitable;
		long blockSize;
		double blockSizeMB, splitSizeMB;
		long blocks, nsplits;
		int splitsPerCore;
		FileSystem srcFS = null, dstFS = null;

		// Check input paths
		srcFS = FileSystem.get(hadoopConfig);
		inputPath1 = srcFS.resolvePath(new Path(options.getInputFile1()));
		inputPath1Length = srcFS.getFileStatus(inputPath1).getLen();
		inputPath1LengthMB = IOUtils.ByteToMiB(inputPath1Length);
		isInputPath1Compressed = IOUtils.isPathCompressed(hadoopConfig, inputPath1);
		isInputPath1Splitable = IOUtils.isInputPathSplitable(hadoopConfig, inputPath1);
		blockSize = srcFS.getFileStatus(inputPath1).getBlockSize();
		blockSizeMB = IOUtils.ByteToMiB(blockSize);
		blocks = SingleEndSequenceInputFormat.getNumberOfSplits(inputPath1, inputPath1Length, isInputPath1Splitable, blockSize);
		splitsPerCore = options.getSplitsPerCore();
		nsplits = parallelism*splitsPerCore;
		splitSize = 0;

		logger.info("INIT: blocks {}, blockSize {}, nsplits {}, splitsPerCore {}, splitSize {}", blocks, blockSize, nsplits, splitsPerCore, splitSize);

		if (blocks >= nsplits) {
			if (blocks > nsplits) {
				IOUtils.warn(String.format("the number of splits (%d) is lower than "
						+ "the number of HDFS blocks (%d), which can affect performance. "
						+ "Creating %d splits instead", nsplits, blocks, blocks));

			}

			nsplits = blocks;
			splitSize = blockSize;
		}

		if (nsplits == 1)
			splitSize = inputPath1Length;
		else if (splitSize == 0) {
			if (inputPath1Length < Configuration.MIN_SPLIT_SIZE) {
				splitSize = inputPath1Length;
				nsplits = 1;
			} else {
				splitSize = (long) IOUtils.divideRoundUp(inputPath1Length, nsplits);

				if (splitSize < Configuration.MIN_SPLIT_SIZE) {
					IOUtils.warn(String.format("the split size (%d bytes) is lower than "
							+ "the minimum value. Using %d MiBytes instead", splitSize, Configuration.MIN_SPLIT_SIZE_MiB));
					splitSize = Configuration.MIN_SPLIT_SIZE;
					nsplits = IOUtils.divideRoundUp(inputPath1Length, splitSize);
				}
			}
		}

		// Set split size and replication factor
		hadoopConfig.setLong(FileInputFormat.SPLIT_MINSIZE, splitSize);
		hadoopConfig.setLong(FileInputFormat.SPLIT_MAXSIZE, splitSize);
		hadoopConfig.set("mapreduce.fileoutputcommitter.algorithm.version", "2");
		hadoopConfig.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, config.HDFS_BLOCK_REPLICATION);
		splitSizeMB = IOUtils.ByteToMiB(splitSize);

		logger.info("END: blocks {}, blockSize {}, nsplits {}, splitsPerCore {}, splitSize {}", blocks, blockSize, nsplits, splitsPerCore, splitSize);

		if (options.getInputFile2() != null) {
			inputPath2 = srcFS.resolvePath(new Path(options.getInputFile2()));
			inputPath2Length = srcFS.getFileStatus(inputPath2).getLen();
			inputPath2LengthMB = IOUtils.ByteToMiB(inputPath2Length);
			isInputPath2Compressed = IOUtils.isPathCompressed(hadoopConfig, inputPath2);
			isInputPath2Splitable = IOUtils.isInputPathSplitable(hadoopConfig, inputPath2);

			// Sanity checks
			if ((isInputPath1Compressed && !isInputPath2Compressed) || (!isInputPath1Compressed && isInputPath2Compressed))
				throw new UnsupportedOperationException("Both input files must be compressed or not compressed");

			if (isInputPath1Compressed && isInputPath2Compressed) {
				if ((isInputPath1Splitable && !isInputPath2Splitable) || (!isInputPath1Splitable && isInputPath2Splitable))
					throw new UnsupportedOperationException("Both compressed input files must be splitable or not splitable");
			}

			if (!isInputPath1Compressed && !isInputPath2Compressed) {
				if (inputPath1Length != inputPath2Length)
					throw new IOException("Input files must be the same size in paired-end mode");
			}
		}

		// Check output paths
		if (options.getOutputDir() == null)
			basePath = new Path(srcFS.getHomeDirectory().toString());
		else
			basePath = new Path(options.getOutputDir());

		if (srcFS.exists(basePath)) {
			if (!srcFS.isDirectory(basePath)) 
				throw new RuntimeException("Output path is invalid: "+basePath+" is not a directory");
		} else {
			srcFS.mkdirs(basePath);
		}

		String concatPath = srcFS.resolvePath(basePath).toString();

		if (basePath.isRoot())
			concatPath = concatPath.substring(0, concatPath.length() - 1);

		outputPath = new Path(concatPath+Configuration.SLASH+RunEC.APP_NAME);
		OUTPUT_PATH1 = new Path(outputPath+Configuration.SLASH+"output1");
		OUTPUT_PATH2 = new Path(outputPath+Configuration.SLASH+"output2");
		options.setOutputDir(outputPath.toString());

		if (srcFS.exists(outputPath))
			srcFS.delete(outputPath, true);
		if (srcFS.exists(OUTPUT_PATH1))
			srcFS.delete(OUTPUT_PATH1, true);

		if (options.isPaired()) {
			if (srcFS.exists(OUTPUT_PATH2))
				srcFS.delete(OUTPUT_PATH2, true);
		}

		if (options.getMergeOutputDir() != null) {
			mergeOutputPath = new Path(options.getMergeOutputDir());
			dstFS = mergeOutputPath.getFileSystem(hadoopConfig);

			if (dstFS.exists(mergeOutputPath)) {
				if(!dstFS.isDirectory(mergeOutputPath)) 
					throw new RuntimeException("Merge output path is invalid: "+mergeOutputPath+" is not a directory");

				dstFS.delete(mergeOutputPath, true);
			}

			dstFS.mkdirs(mergeOutputPath);
			mergeOutputPath = dstFS.resolvePath(mergeOutputPath);
			options.setMergeOutputDir(mergeOutputPath.toString());
		} else {
			dstFS = FileSystem.get(hadoopConfig);
			options.setMergeOutputDir(outputPath.toString());
		}

		IOUtils.info("HDFS block replication = "+config.HDFS_BLOCK_REPLICATION);
		IOUtils.info("HDFS block size = "+IOUtils.formatOneDecimal(blockSizeMB)+" MiBytes");

		if (inputPath1LengthMB > 1)
			IOUtils.info("input file = "+inputPath1+" ("+IOUtils.formatOneDecimal(inputPath1LengthMB)+" MiBytes)");
		else
			IOUtils.info("input file = "+inputPath1+" ("+inputPath1Length+" bytes)");

		if (inputPath2 != null) {
			if (inputPath2LengthMB > 1)
				IOUtils.info("input file = "+inputPath2+" ("+IOUtils.formatOneDecimal(inputPath2LengthMB)+" MiBytes)");
			else
				IOUtils.info("input file = "+inputPath2+" ("+inputPath2Length+" bytes)");
		}

		IOUtils.info("output directory = "+outputPath);
		if (mergeOutputPath != null) 
			IOUtils.info("merge output directory = "+mergeOutputPath);
		IOUtils.info("HDFS blocks = "+blocks);
		IOUtils.info("parallelism = "+parallelism+" cores");
		IOUtils.info("splits per core = "+splitsPerCore);
		IOUtils.info("splits = "+nsplits);
		if (splitSizeMB >= 1)
			IOUtils.info("split size = "+IOUtils.formatOneDecimal(splitSizeMB)+" MiBytes");
		else
			IOUtils.info("split size = "+splitSize+" bytes");

		return nsplits;
	}

	public static FileFormat getInputFileFormat(org.apache.hadoop.conf.Configuration hadoopConf, Path inputPath) throws IOException {
		FileSystem fs = FileSystem.get(hadoopConf);
		FileFormat format = FileFormat.FILE_FORMAT_UNKNOWN;

		// Try to autodetect the input file format
		IOUtils.info("detecting input file format from "+inputPath);

		if (IOUtils.isPathCompressed(hadoopConf, inputPath)) {
			IOUtils.info("input file is compressed, assuming FASTQ format");
			return FileFormat.FILE_FORMAT_FASTQ;
		}

		format = SequenceParserFactory.autoDetectFileFormat(fs, inputPath);

		if (format == FileFormat.FILE_FORMAT_FASTQ) {
			IOUtils.info("FASTQ format identified");
			format = FileFormat.FILE_FORMAT_FASTQ;
		} else if (format == FileFormat.FILE_FORMAT_FASTA) {
			IOUtils.info("FASTA format identified");
			format = FileFormat.FILE_FORMAT_FASTA;
		}

		return format;
	}

	public static long writeHistogram(FileSystem fs, Path outputFile, int[] histogram, int offset, int length) throws IOException {
		FSDataOutputStream out = null;
		long total = 0;
		StringBuilder sb = new StringBuilder(length);

		try {
			if (fs.exists(outputFile)) {
				for (int i = offset; i < length; i++) {
					total += histogram[i];
				}
				return total;
			}

			IOUtils.info("writing histogram to "+outputFile);

			out = fs.create(outputFile);

			for (int i = offset; i < length; i++) {
				sb.append(i+SEP_CHAR).append(String.valueOf(histogram[i])).append(NEW_LINE);
				out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
				total += histogram[i];
				sb.setLength(0);
			}
		} finally {
			if (out != null)
				out.close();
		}

		return total;
	}

	public static int[] loadHistogram(FileSystem fs, Path path, int size, boolean delete) throws IOException {
		BufferedReader br = null;
		FSDataInputStream dis = null;
		String line;
		Path file;
		int[] histogram = new int[size];
		int i = 0;

		try {
			if (!fs.exists(path))
				IOUtils.error("Histogram file path not found: "+path);

			if (fs.isDirectory(path))
				file = new Path(path+Configuration.SLASH+fs.listStatus(path)[0].getPath().getName());				
			else
				file = path;

			if (!fs.exists(file))
				IOUtils.error("Histogram file file not found: "+file);

			logger.info("loading histogram from {}", file);

			dis = fs.open(file);
			br = new BufferedReader(new InputStreamReader(dis));
			line = br.readLine();

			while (line != null) {
				if (!line.isEmpty())
					histogram[i++] = Integer.parseInt(line.split(SEP_CHAR)[1]);

				line = br.readLine();
			}
		} finally {
			if (br != null)
				br.close();
			if (dis != null)
				dis.close();
		}

		if (delete)
			fs.delete(file, true);

		return histogram;
	}

	public static int getFirstSequenceSize(FileSystem fs, Path file) throws IOException {
		BufferedReader br = null;
		FSDataInputStream dis = null;
		String line;
		StringBuilder sb = new StringBuilder(250);

		dis = fs.open(file);
		br = new BufferedReader(new InputStreamReader(dis));

		while (true) {
			// Name
			line = br.readLine();
			sb.append(line);
			// Bases
			line = br.readLine();
			sb.append(line);
			// Connector
			line = br.readLine();
			sb.append(line);
			// Quality scores
			line = br.readLine();
			sb.append(line);
			break;
		}

		if (br != null)
			br.close();
		if (dis != null)
			dis.close();

		return sb.toString().length() + 15;
	}

	public static SingleEndSequenceInputFormat getInputFormatInstance(Class<? extends SingleEndSequenceInputFormat> inputFormatClass) {
		Object obj = null;

		try {
			obj = inputFormatClass.getConstructor().newInstance();
		} catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			IOUtils.error(e.getMessage());
		}

		return (SingleEndSequenceInputFormat) obj;
	}

	public static boolean isPathCompressed(org.apache.hadoop.conf.Configuration conf, Path path) {
		CompressionCodec codec = getCompressionCodec(conf, path);

		if (codec == null)
			return false;

		return true;
	}

	public static CompressionCodec getCompressionCodec(org.apache.hadoop.conf.Configuration conf, Path path) {
		return new CompressionCodecFactory(conf).getCodec(path);
	}

	public static void enableOutputCompression(String codec, org.apache.hadoop.conf.Configuration conf) {
		logger.info("Enabled output compression using codec: {}", codec);
		conf.setBoolean("mapreduce.output.fileoutputformat.compress", true);
		conf.set("mapreduce.output.fileoutputformat.compress.codec", codec);
	}

	private static boolean isInputPathSplitable(org.apache.hadoop.conf.Configuration conf, Path inputPath) {
		CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(inputPath);

		if (codec == null)
			return true;

		return codec instanceof SplittableCompressionCodec;
	}
}
