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
package es.udc.gac.bigdec.ec.flink;

import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.SafetyNetWrapperFileSystem;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;

public class TextOuputFormat<T> extends FileOutputFormat<T> {

	private static final long serialVersionUID = -1041067889189124060L;
	private static final Logger logger = LoggerFactory.getLogger(TextOuputFormat.class);
	private static final int NEWLINE = '\n';

	private String charsetName;
	private transient Charset charset;
	private transient boolean fileCreated;
	private transient Path actualFilePath;
	private int replication;

	public interface TextFormatter<IN> extends Serializable {
		String format(IN value);
	}

	public TextOuputFormat(Path outputPath) {
		this(outputPath, "UTF-8");
		this.replication = DFSConfigKeys.DFS_REPLICATION_DEFAULT;
	}

	public TextOuputFormat(Path outputPath, org.apache.hadoop.conf.Configuration hadoopConfig) {
		this(outputPath, "UTF-8");
		this.replication = hadoopConfig.getInt(DFSConfigKeys.DFS_REPLICATION_KEY, DFSConfigKeys.DFS_REPLICATION_DEFAULT);
	}

	public TextOuputFormat(Path outputPath, String charset) {
		super(outputPath);
		this.charsetName = charset;
		this.replication = DFSConfigKeys.DFS_REPLICATION_DEFAULT;
	}

	public TextOuputFormat(Path outputPath, String charset, org.apache.hadoop.conf.Configuration hadoopConfig) {
		super(outputPath);
		this.charsetName = charset;
		this.replication = hadoopConfig.getInt(DFSConfigKeys.DFS_REPLICATION_KEY, DFSConfigKeys.DFS_REPLICATION_DEFAULT);
	}

	public String getCharsetName() {
		return charsetName;
	}

	public void setCharsetName(String charsetName)
			throws IllegalCharsetNameException, UnsupportedCharsetException {
		if (charsetName == null) {
			throw new NullPointerException();
		}

		if (!Charset.isSupported(charsetName)) {
			throw new UnsupportedCharsetException(
					"The charset " + charsetName + " is not supported.");
		}

		this.charsetName = charsetName;
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		if (taskNumber < 0 || numTasks < 1) {
			throw new IllegalArgumentException(
					"TaskNumber: " + taskNumber + ", numTasks: " + numTasks);
		}

		Path p = this.outputFilePath;
		if (p == null) {
			throw new IOException("The file path is null");
		}

		final FileSystem fs = p.getFileSystem();

		// if this is a local file system, we need to initialize the local output directory here
		if (!fs.isDistributedFS()) {
			if (numTasks == 1 && getOutputDirectoryMode() == OutputDirectoryMode.PARONLY) {
				// output should go to a single file

				// prepare local output path. checks for write mode and removes existing files in
				// case of OVERWRITE mode
				if (!fs.initOutPathLocalFS(p, getWriteMode(), false)) {
					// output preparation failed! Cancel task.
					throw new IOException(
							"Output path '"
									+ p.toString()
									+ "' could not be initialized. Canceling task...");
				}
			} else {
				// numTasks > 1 || outDirMode == OutputDirectoryMode.ALWAYS

				if (!fs.initOutPathLocalFS(p, getWriteMode(), true)) {
					// output preparation failed! Cancel task.
					throw new IOException(
							"Output directory '"
									+ p.toString()
									+ "' could not be created. Canceling task...");
				}
			}
		}

		// Suffix the path with the parallel instance index, if needed
		this.actualFilePath =
				(numTasks > 1 || getOutputDirectoryMode() == OutputDirectoryMode.ALWAYS)
				? p.suffix("/" + getDirectoryFileName(taskNumber))
						: p;

		this.stream = null;

		// create output file
		if (fs.isDistributedFS() && fs instanceof SafetyNetWrapperFileSystem) {
			FileSystem wFS = ((SafetyNetWrapperFileSystem) fs).getWrappedDelegate();

			if (wFS instanceof HadoopFileSystem) {
				HadoopFileSystem dFS = ((HadoopFileSystem) wFS);
				org.apache.hadoop.conf.Configuration hadoopConfig = dFS.getHadoopFileSystem().getConf();
				long blockSize = hadoopConfig.getLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
				int bufferSize = hadoopConfig.getInt(DFSConfigKeys.IO_FILE_BUFFER_SIZE_KEY, DFSConfigKeys.IO_FILE_BUFFER_SIZE_DEFAULT);
				logger.info("HDFS detected: bufferSize {}, blockSize {}, replication {}", bufferSize, blockSize, replication);

				this.stream = dFS.create(this.actualFilePath, true,	bufferSize, (short) replication, blockSize);
			}
		}

		if (this.stream == null)
			this.stream = fs.create(this.actualFilePath, getWriteMode());

		// at this point, the file creation must have succeeded, or an exception has been thrown
		this.fileCreated = true;

		try {
			this.charset = Charset.forName(charsetName);
		} catch (IllegalCharsetNameException e) {
			throw new IOException("The charset " + charsetName + " is not valid", e);
		} catch (UnsupportedCharsetException e) {
			throw new IOException("The charset " + charsetName + " is not supported", e);
		}
	}

	@Override
	public void writeRecord(T record) throws IOException {
		this.stream.write(record.toString().getBytes(charset));
		this.stream.write(NEWLINE);
	}

	@Override
	public String toString() {
		return "TextOF (" + getOutputFilePath() + ") - " + this.charsetName;
	}

	@Override
	public void tryCleanupOnError() {
		if (this.fileCreated) {
			this.fileCreated = false;

			try {
				close();
			} catch (IOException e) {
				logger.warn("Could not properly close FileOutputFormat", e);
			}

			try {
				FileSystem.get(this.actualFilePath.toUri()).delete(actualFilePath, false);
			} catch (FileNotFoundException e) {
				// ignore, may not be visible yet or may be already removed
			} catch (Throwable t) {
				logger.warn("Could not remove the incomplete file " + actualFilePath, t);
			}
		}
	}
}