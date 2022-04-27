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

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.udc.gac.bigdec.RunEC;
import es.udc.gac.bigdec.RunMerge;
import es.udc.gac.bigdec.util.Configuration;
import es.udc.gac.bigdec.util.IOUtils;

public class MergerThread extends Thread {

	private static final Logger logger = LoggerFactory.getLogger(MergerThread.class);
	private static final int SLEEP_LS = 2000;
	private static final int SLEEP_EXISTS = 1000;
	private static final int SLEEP_EOF = 2000;
	private static final double SLEEP_RATIO = 0.25; //25%
	private static final int SLEEP_STE = 2000;
	private static final int MAX_RETRIES = 3;

	private Configuration config;
	private List<Path> inputPaths;
	private BlockingQueue<Path> inputPathsQueue;
	private long outputFiles;
	private AtomicBoolean running;
	private byte buffer[];
	private FileSystem srcFS;
	private FileSystem dstFS;
	private Path outputFile1;
	private Path outputFile2;
	private long blockSize;
	private long fileSize;
	private Path done;
	private boolean asynchronous;
	private int increaseSleep;
	private long sleepsLS;
	private long sleepsEOF;
	private long sleepsSTE;

	public MergerThread(FileSystem srcFS, FileSystem dstFS, List<Path> inputPaths, BlockingQueue<Path> inputPathsQueue,
			Path outputFile1, Path outputFile2, long outputFiles, Path done, boolean asynchronous, long fileSize,
			long blockSize, Configuration config, org.apache.hadoop.conf.Configuration hadoopConfig) {
		this.config = config;
		this.srcFS = srcFS;
		this.dstFS = dstFS;
		this.outputFile1 = outputFile1;
		this.outputFile2 = outputFile2;
		this.inputPaths = inputPaths;
		this.inputPathsQueue = inputPathsQueue;
		this.outputFiles = outputFiles;
		int bufferSize = hadoopConfig.getInt(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY, RunMerge.BUFFER_SIZE_DEFAULT);
		this.buffer = new byte[bufferSize];
		this.blockSize = blockSize;
		this.fileSize = fileSize;
		this.running = new AtomicBoolean(false);
		this.done = done;
		this.asynchronous = asynchronous;
		this.increaseSleep = (int) (SLEEP_EOF * SLEEP_RATIO);
		this.sleepsLS = 0;
		this.sleepsEOF = 0;
		this.sleepsSTE = 0;
	}

	public void terminate() {
		running.set(false);
		if (currentThread().isAlive())
			interrupt();
	}

	public void put(Path path) throws InterruptedException {
		if (inputPathsQueue != null)
			inputPathsQueue.put(path);
	}

	@Override
	public void run() {
		running.set(true);

		logger.info("bufferSize {}, blockSize {}, replication factor {}", buffer.length, blockSize, config.HDFS_BLOCK_REPLICATION.shortValue());

		if (logger.isDebugEnabled()) {
			logger.debug("Paths to merge");
			for (Path path: inputPaths) {
				logger.debug(path.toString());
			}
		}

		while (running.get()) {
			try {
				if (RunEC.EXECUTION_ENGINE == RunEC.ExecutionEngine.FLINK_MODE) {
					if (asynchronous)
						asynchronousMerge(1);
					else
						synchronousMerge();
				} else {
					if (asynchronous) {
						if (config.SPARK_API.equalsIgnoreCase("RDD"))
							asynchronousMerge("part-", null, 0);
						else
							asynchronousMerge("part-", "[-]\\S*", 0);
					} else {
						synchronousMerge();
					}
				}

				if (config.HDFS_DELETE_TEMP_FILES) {
					for(Path path: inputPaths)
						srcFS.delete(path, true);
				}

				break;
			} catch (InterruptedException ie) {
				running.set(false);
				throw new RuntimeException(ie.getMessage());
			} catch (IOException ioe) {
				running.set(false);
				throw new RuntimeException(ioe.getMessage());
			}
		}

		running.set(false);
		logger.info("MergerThread finished (sleeps: ls {}, eof {}, ste {})", sleepsLS, sleepsEOF, sleepsSTE);
	}

	private void synchronousMerge() throws IOException, InterruptedException {
		Path outputFile;
		FSDataOutputStream out;
		List<Path> inputFiles;
		Path inputPath;

		logger.info("SynchronousMerge");

		if (inputPathsQueue == null)
			throw new IOException("Input blocking queue is null");

		while (true) {
			logger.info("Waiting to receive signal from main thread");
			// Wait until notified from main thread
			inputPath = inputPathsQueue.take();

			if (inputPath.equals(done)) {
				logger.info("Nothing to do");
				break;
			}

			while (!srcFS.exists(inputPath))
				Thread.sleep(SLEEP_EXISTS);

			if (inputPath.getParent().equals(IOUtils.getOutputPath1()))
				outputFile = new Path(outputFile1+"."+inputPath.getName());
			else
				outputFile = new Path(outputFile2+"."+inputPath.getName());

			logger.info("Merging from {} to {}", inputPath, outputFile);

			// Create output file
			out = dstFS.create(outputFile, true, buffer.length, config.HDFS_BLOCK_REPLICATION.shortValue(), blockSize);

			// Get input files
			inputFiles = RunMerge.getFiles(srcFS, inputPath, config.KEEP_ORDER);

			// Copy files
			for (Path inputFile: inputFiles)
				copyFile(inputFile, out);

			logger.info("Copied {} files out of {} ({})", inputFiles.size(), outputFiles, inputPath);

			out.close();
		}
	}

	private void asynchronousMerge(String prefix, String suffix, int firstFileNumber) throws IOException, InterruptedException {
		Path outputFile;
		int processedFiles, i;
		List<MutablePair<Path,Integer>> filesToProcess = new ArrayList<MutablePair<Path,Integer>>();
		ListIterator<MutablePair<Path,Integer>> iter;
		List<Path> filesProcessed = new ArrayList<Path>();
		List<Path> filesReadyToProcess = new ArrayList<Path>();
		FileStatus[] inputFiles;
		FileStatus file;
		FSDataOutputStream out;
		Path pattern;
		StringBuilder fileName = new StringBuilder(64);

		logger.info("AsynchronousMerge (prefix {}, suffix {}, fileSize {})", prefix, suffix, fileSize);

		if (inputPathsQueue != null) {
			logger.info("Waiting to receive signal from main thread");
			// Wait until notified from main thread
			inputPathsQueue.take();
		}

		PathFilter filter = new PathFilter() {
			public boolean accept(Path file) {
				return (!filesProcessed.contains(file) && !filesReadyToProcess.contains(file));
			}
		};

		for (Path inputPath: inputPaths) {
			if (inputPath.getParent().equals(IOUtils.getOutputPath1()))
				outputFile = new Path(outputFile1+"."+inputPath.getName());
			else
				outputFile = new Path(outputFile2+"."+inputPath.getName());

			while (!srcFS.exists(inputPath))
				Thread.sleep(SLEEP_EXISTS);

			pattern = new Path(inputPath+Configuration.SLASH+prefix+"*");
			logger.info("Merging from {} to {}", pattern, outputFile);

			// Create output file
			out = dstFS.create(outputFile, true, buffer.length, config.HDFS_BLOCK_REPLICATION.shortValue(), blockSize);

			processedFiles = 0;
			filesToProcess.clear();
			filesProcessed.clear();
			filesReadyToProcess.clear();
			int nextFile = firstFileNumber;
			Thread.sleep(SLEEP_LS);

			while (processedFiles < outputFiles) {
				// Try to get new input files to process
				inputFiles = srcFS.globStatus(pattern, filter);

				for (i = 0; i < inputFiles.length; i++) {
					file = inputFiles[i];
					fileName.setLength(0);

					if (prefix != null) {
						if (suffix != null)
							fileName.append(file.getPath().getName().replaceFirst(prefix, "").replaceFirst(suffix, ""));
						else
							fileName.append(file.getPath().getName().replaceFirst(prefix, ""));
					} else {
						fileName.append(file.getPath().getName());
					}

					filesToProcess.add(new MutablePair<Path,Integer>(file.getPath(), Integer.parseInt(fileName.toString())));
					filesReadyToProcess.add(file.getPath());
				}

				// No files available
				if (filesToProcess.size() == 0 || inputFiles.length == 0) {
					Thread.sleep(SLEEP_LS);
					sleepsLS++;
					continue;
				}

				if (config.KEEP_ORDER) {
					// Sort list by file number
					filesToProcess.sort(new Comparator<MutablePair<Path,Integer>>() {
						@Override
						public int compare(MutablePair<Path,Integer> p1, MutablePair<Path,Integer> p2) {
							return p1.getRight().compareTo(p2.getRight());
						}
					});
				}

				logger.info("{} input files, {} files to process (next file = {})", inputFiles.length, filesToProcess.size(), nextFile);

				if (logger.isDebugEnabled()) {
					for (MutablePair<Path,Integer> pair: filesToProcess)
						logger.debug("{} -> {}", pair.right, pair.left);
				}

				iter = filesToProcess.listIterator();

				if (config.KEEP_ORDER) {
					while(iter.hasNext()) {
						MutablePair<Path,Integer> currentFile = iter.next();

						if (currentFile.getRight() == nextFile) {
							copyFile(currentFile.getLeft(), out);
							filesProcessed.add(currentFile.getLeft());
							filesReadyToProcess.remove(currentFile.getLeft());
							iter.remove();
							processedFiles++;
							nextFile++;
						} else {
							break;
						}
					}
				} else {
					while(iter.hasNext()) {
						Path currentFile = iter.next().getLeft();
						copyFile(currentFile, out);
						filesProcessed.add(currentFile);
						filesReadyToProcess.remove(currentFile);
						iter.remove();
						processedFiles++;
					}
				}

				logger.info("Copied {} files out of {} ({})", processedFiles, outputFiles, inputPath);
			}

			out.close();
		}
	}

	private void asynchronousMerge(int firstFileNumber) throws IOException, InterruptedException {
		Path outputFile;
		List<Path> filesToProcess = new ArrayList<Path>();
		FSDataOutputStream out;

		logger.info("AsynchronousMerge (fileSize {})", fileSize);

		if (inputPathsQueue != null) {
			logger.info("Waiting to receive signal from main thread");
			// Wait until notified from main thread
			inputPathsQueue.take();
		}

		for (Path inputPath: inputPaths) {
			if (inputPath.getParent().equals(IOUtils.getOutputPath1()))
				outputFile = new Path(outputFile1+"."+inputPath.getName());
			else
				outputFile = new Path(outputFile2+"."+inputPath.getName());

			while (!srcFS.exists(inputPath))
				Thread.sleep(SLEEP_EXISTS);

			logger.info("Merging from {} to {}", inputPath, outputFile);

			// Create output file
			out = dstFS.create(outputFile, true, buffer.length, config.HDFS_BLOCK_REPLICATION.shortValue(), blockSize);

			filesToProcess.clear();

			// Create input paths
			for (int i = firstFileNumber; i <= outputFiles; i++)
				filesToProcess.add(new Path(inputPath+Configuration.SLASH+i));

			// Copy files
			for (Path path: filesToProcess) {
				while (!srcFS.exists(path))
					Thread.sleep(SLEEP_EXISTS);

				fullyCopyFile(path, out, fileSize);
			}

			logger.info("Copied {} files out of {} ({})", filesToProcess.size(), outputFiles, inputPath);

			out.close();
		}
	}

	private long copyFile(Path inputFile, FSDataOutputStream out) throws IOException, InterruptedException {
		FSDataInputStream in = srcFS.open(inputFile);
		long totalBytes = 0;
		int bytesRead = 0;

		while (bytesRead >= 0) {
			try {
				bytesRead = in.read(buffer, 0, buffer.length);
			} catch (SocketTimeoutException e) {
				handleSocketTimeout(inputFile);
				continue;
			}

			if (bytesRead > 0) {
				out.write(buffer, 0, bytesRead);
				totalBytes += bytesRead;
			}
		}

		in.close();
		logger.debug("Copied {} ({} bytes)", inputFile, totalBytes);

		return totalBytes;
	}

	private long fullyCopyFile(Path inputFile, FSDataOutputStream out, long fileSize) throws IOException, InterruptedException {
		FSDataInputStream in = srcFS.open(inputFile);
		long totalBytes = 0;
		int bytesToRead = 0;
		int sleep = SLEEP_EOF;

		logger.debug("Copying {} (incrSleep {})", inputFile, increaseSleep);

		while (bytesToRead >= 0) {
			try {
				bytesToRead = in.read(buffer, 0, buffer.length);
			} catch (SocketTimeoutException e) {
				handleSocketTimeout(inputFile);
				continue;
			}

			if (bytesToRead > 0) {
				out.write(buffer, 0, bytesToRead);
				totalBytes += bytesToRead;
			}
		}

		// EOF
		while (totalBytes < fileSize) {
			while (bytesToRead <= 0) {
				in.close();
				logger.debug("EOF (sleep {}, {})", sleep, inputFile);
				Thread.sleep(sleep);
				sleepsEOF++;
				in = srcFS.open(inputFile);

				try {
					if (totalBytes > 0)
						in.seek(totalBytes);
				} catch (EOFException eof) {
					sleep += increaseSleep;
					continue;
				}

				bytesToRead = in.available();
			}

			sleep = SLEEP_EOF;
			logger.debug("totalBytes {}, bytesToRead {} ({})",  totalBytes, bytesToRead, inputFile);

			while (bytesToRead >= 0) {
				try {
					bytesToRead = in.read(buffer, 0, buffer.length);
				} catch (SocketTimeoutException e) {
					handleSocketTimeout(inputFile);
					continue;
				}

				if (bytesToRead > 0) {
					out.write(buffer, 0, bytesToRead);
					totalBytes += bytesToRead;
				}
			}
		}

		in.close();

		logger.info("Copied {} ({} bytes)", inputFile, totalBytes);

		return totalBytes;
	}

	private void handleSocketTimeout(Path inputFile) throws IOException, InterruptedException {
		logger.warn("SocketTimeoutException copying {}", inputFile);

		if (sleepsSTE == MAX_RETRIES)
			throw new IOException("Max retries ("+sleepsSTE+") exceeded for SocketTimeoutException");


		sleepsSTE++;
		Thread.sleep(SLEEP_STE);
	}
}
