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
package es.udc.gac.bigdec;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.udc.gac.bigdec.util.IOUtils;
import es.udc.gac.bigdec.util.Timer;

public class RunMerge extends Configured implements Tool {

	private static final long BLOCK_SIZE_DEFAULT = 128 * 1024 * 1024;
	private static final int BUFFER_SIZE_DEFAULT = 64 * 1024;

	private static final Logger logger = LoggerFactory.getLogger(RunMerge.class);
	private static final Timer timer = new Timer();
	private static final String TOTAL_TIME = "TOTAL_TIME";
	private static byte buffer[];
	private static String inputDir;
	private static String outputFile;
	private static int replication = 1;
	private static boolean deleteSrc = false;

	private static long copyBytes(Path inputFile, FSDataInputStream in, FSDataOutputStream out, byte buffer[],
			int bufferSize) throws IOException {

		long totalBytes = 0;
		int bytesRead = 0;

		while (bytesRead >= 0) {
			bytesRead = in.read(buffer, 0, bufferSize);

			if (bytesRead > 0) {
				out.write(buffer, 0, bytesRead);
				totalBytes += bytesRead;
			}
		}
		return totalBytes;
	}

	public static List<Path> getFiles(FileSystem srcFS, Path srcDir, boolean sort) throws FileNotFoundException, IOException {

		if (!srcFS.getFileStatus(srcDir).isDirectory())
			throw new IOException("Input source path is not a directory");

		List<FileStatus> contents = new ArrayList<FileStatus>(Arrays.asList(srcFS.listStatus(srcDir)));
		List<Path> files = new ArrayList<Path>(contents.size());

		for (FileStatus file: contents) {
			if (file.getLen() <= 0 || !file.isFile())
				continue;

			logger.debug("file: {} ({} bytes)", file.getPath().getName(), file.getLen());

			files.add(file.getPath());
		}

		if (sort) {
			files.sort((o1, o2) -> o1.compareTo(o2));

			files.sort(new Comparator<Path>() {
				public int compare(Path f1, Path f2) {
					try {
						int i1 = Integer.parseInt(f1.getName());
						int i2 = Integer.parseInt(f2.getName());
						return i1 - i2;
					} catch(NumberFormatException e) {
						return 0;
					}
				}
			});
		}

		return files;
	}

	public static List<Path> getFiles(FileSystem srcFS, Path srcDir) throws FileNotFoundException, IOException {
		return RunMerge.getFiles(srcFS, srcDir, true);
	}

	public static void merge(FileSystem srcFS, List<Path> inputFiles, FileSystem dstFS, Path dstFile, int bufferSize, int replication,
			long blockSize, org.apache.hadoop.conf.Configuration conf) throws IOException {

		FSDataOutputStream out = dstFS.create(dstFile, true, bufferSize, (short) replication, blockSize);
		FSDataInputStream in = null;

		if (buffer == null)
			buffer = new byte[bufferSize];

		IOUtils.info("merging "+inputFiles.size()+" output files");

		try {
			for (Path inputFile: inputFiles) {
				in = srcFS.open(inputFile);
				RunMerge.copyBytes(inputFile, in, out, buffer, bufferSize);
				in.close();
			}
		} finally {
			out.close();
		}
	}

	public static void merge(FileSystem srcFS, Path srcDir, List<Path> inputFiles, FileSystem dstFS, Path dstFile, int replication,
			boolean deleteSource, org.apache.hadoop.conf.Configuration conf) throws IOException {

		long blockSize = conf.getLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE_DEFAULT);
		int bufferSize = conf.getInt(DFSConfigKeys.IO_FILE_BUFFER_SIZE_KEY, BUFFER_SIZE_DEFAULT);
		RunMerge.merge(srcFS, inputFiles, dstFS, dstFile, bufferSize, replication, blockSize, conf);

		if (deleteSource)
			srcFS.delete(srcDir, true);
	}

	public static void merge(FileSystem srcFS, Path srcDir, List<Path> inputFiles, FileSystem dstFS, Path dstFile, int replication,
			long blockSize, boolean deleteSource, org.apache.hadoop.conf.Configuration conf) throws IOException {

		int bufferSize = conf.getInt(DFSConfigKeys.IO_FILE_BUFFER_SIZE_KEY, BUFFER_SIZE_DEFAULT);

		RunMerge.merge(srcFS, inputFiles, dstFS, dstFile, bufferSize, replication, blockSize, conf);

		if (deleteSource)
			srcFS.delete(srcDir, true);
	}

	public static void merge(FileSystem srcFS, Path srcDir, FileSystem dstFS, Path dstFile, int bufferSize, int replication,
			long blockSize, boolean deleteSource, org.apache.hadoop.conf.Configuration conf) throws IOException {

		List<Path> inputFiles = RunMerge.getFiles(srcFS, srcDir, true);

		RunMerge.merge(srcFS, inputFiles, dstFS, dstFile, bufferSize, replication, blockSize, conf);

		if (deleteSource)
			srcFS.delete(srcDir, true);
	}

	public static void merge(FileSystem srcFS, Path srcDir, FileSystem dstFS, Path dstFile, int bufferSize, int replication, 
			boolean deleteSource, org.apache.hadoop.conf.Configuration conf) throws IOException {

		long blockSize = conf.getLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE_DEFAULT);
		RunMerge.merge(srcFS, srcDir, dstFS, dstFile, bufferSize, replication, blockSize, deleteSource, conf);
	}

	public static void merge(FileSystem srcFS, Path srcDir, FileSystem dstFS, Path dstFile, long blockSize, int replication, 
			boolean deleteSource, org.apache.hadoop.conf.Configuration conf) throws IOException {

		int bufferSize = conf.getInt(DFSConfigKeys.IO_FILE_BUFFER_SIZE_KEY, BUFFER_SIZE_DEFAULT);
		RunMerge.merge(srcFS, srcDir, dstFS, dstFile, bufferSize, replication, blockSize, deleteSource, conf);
	}

	public static void merge(FileSystem srcFS, Path srcDir, FileSystem dstFS, Path dstFile, int replication, 
			boolean deleteSource, org.apache.hadoop.conf.Configuration conf) throws IOException {

		long blockSize = conf.getLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE_DEFAULT);
		int bufferSize = conf.getInt(DFSConfigKeys.IO_FILE_BUFFER_SIZE_KEY, BUFFER_SIZE_DEFAULT);
		RunMerge.merge(srcFS, srcDir, dstFS, dstFile, bufferSize, replication, blockSize, deleteSource, conf);
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new org.apache.hadoop.conf.Configuration(), new RunMerge(), args));
	}

	@Override
	public int run(String[] args) throws Exception {

		// Get Hadoop configuration
		org.apache.hadoop.conf.Configuration conf = this.getConf();

		if(!parse(args))
			return -1;

		FileSystem srcFS  = FileSystem.get(conf);
		Path inputPath = srcFS.resolvePath(new Path(inputDir));
		Path outputPath = new Path(outputFile);
		FileSystem dstFS  = outputPath.getFileSystem(conf);

		IOUtils.info("input source path = "+inputPath);

		long blockSize = conf.getLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE_DEFAULT);
		int bufferSize = conf.getInt(DFSConfigKeys.IO_FILE_BUFFER_SIZE_KEY, BUFFER_SIZE_DEFAULT);

		timer.start(TOTAL_TIME);
		RunMerge.merge(srcFS, inputPath, dstFS, outputPath, bufferSize, replication, blockSize, deleteSrc, conf);
		timer.stop(TOTAL_TIME);

		outputPath = outputPath.getFileSystem(conf).resolvePath(outputPath);
		IOUtils.info("merge done in "+IOUtils.formatTwoDecimal(timer.getTotalTime(TOTAL_TIME))+" seconds");

		long outputPathLength = dstFS.getFileStatus(outputPath).getLen();
		double outputPathLengthMB = IOUtils.ByteToMiB(outputPathLength);

		if (outputPathLengthMB > 1)
			IOUtils.info("merged output file = "+outputPath+" ("+IOUtils.formatOneDecimal(outputPathLengthMB)+" MiBytes)");
		else
			IOUtils.info("merged output file = "+outputPath+" ("+outputPathLength+" bytes)");

		return 0;
	}

	private static void printUsage(String name) {
		System.out.println();
		System.out.println(
				"Usage: " + RunMerge.class.getName() + " -i <srcDir> -o <dstFile> [-d] [-r <replication>] [-h]");
		System.out.println();
		System.out.println("Options:");
		System.out.println(
				"    -i <string>  Input source path on HDFS");
		System.out.println(
				"    -o <string>  Output file on HDFS (use file:/ scheme for local file system)");
		System.out.println(
				"    -d           Delete input source path (default = false)");
		System.out.println(
				"    -r <int>     HDFS replication factor for output file (default = 1)");
		System.out.println(
				"    -h           Print the help message and quit");
		System.out.println();
		System.out.println("This tool takes a source directory and destination file as"
				+ " arguments and concatenates all files in <srcDir> path into the <dstFile> file");
	}

	private static boolean parse(String[] args) {
		int argInd = 0;
		boolean withInputDir = false;
		boolean withOutputFile = false;

		while (argInd < args.length) {
			if (args[argInd].equals("-i")) {
				argInd++;
				if (argInd < args.length) {
					inputDir = args[argInd];
					withInputDir = true;
					argInd++;
				} else {
					System.err.println("No value specified for parameter -i");
					return false;
				}
			} else if (args[argInd].equals("-o")) {
				argInd++;
				if (argInd < args.length) {
					outputFile = args[argInd];
					withOutputFile = true;
					argInd++;
				} else {
					System.err.println("No value specified for parameter -o");
					return false;
				}
			} else if (args[argInd].equals("-r")) {
				argInd++;
				if (argInd < args.length) {
					replication = Integer.parseInt(args[argInd]);
					argInd++;
				} else {
					System.err.println("No value specified for parameter -r");
					return false;
				}
			} else if (args[argInd].equals("-d")) {
				deleteSrc = true;
				argInd++;
			} else if (args[argInd].equals("-h")) {
				// Check the help
				printUsage(RunMerge.class.getName());
				return false;
			} else {
				System.err.println("Parameter " + args[argInd] + " is not valid");
				return false;
			}
		}

		if (!withInputDir) {
			System.err.println("No input path specified with -i");
			printUsage(RunMerge.class.getName());
			return false;
		}

		if (!withOutputFile) {
			System.err.println("No output file specified with -o");
			printUsage(RunMerge.class.getName());
			return false;
		}

		if (replication <= 0) {
			System.err.println("Invalid replicationf factor: "+replication);
			return false;
		}

		return true;
	}
}
