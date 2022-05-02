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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.SafetyNetWrapperFileSystem;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.udc.gac.bigdec.kmer.Kmer;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;

public class KmerCsvOutputFormat extends FileOutputFormat<Tuple2<Kmer,Integer>> implements InputTypeConfigurable {

	private static final long serialVersionUID = -6259282333152635727L;
	private static final Logger logger = LoggerFactory.getLogger(KmerCsvOutputFormat.class);
	private static final String DEFAULT_LINE_DELIMITER = CsvInputFormat.DEFAULT_LINE_DELIMITER;
	private static final String DEFAULT_FIELD_DELIMITER = String.valueOf(CsvInputFormat.DEFAULT_FIELD_DELIMITER);

	private transient Writer wrt;
	private transient boolean fileCreated;
	private transient Path actualFilePath;
	private short replication;
	private String charsetName;

	/**
	 * Creates an instance of CsvOutputFormat. Lines are separated by the newline character '\n',
	 * fields are separated by ','.
	 *
	 * @param outputPath The path where the CSV file is written.
	 */
	public KmerCsvOutputFormat(Path outputPath) {
		super(outputPath);
		this.replication = DFSConfigKeys.DFS_REPLICATION_DEFAULT;
		this.charsetName = null;
	}

	public KmerCsvOutputFormat(Path outputPath, short replication) {
		super(outputPath);
		this.replication = replication;
		this.charsetName = null;
	}

	public KmerCsvOutputFormat(String outputPath, short replication) {
		super(new Path(outputPath));
		this.replication = replication;
		this.charsetName = null;
	}

	public String getCharsetName() {
		return charsetName;
	}

	public void setCharsetName(String charsetName) throws IllegalCharsetNameException, UnsupportedCharsetException {
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
				(numTasks > 1 || getOutputDirectoryMode() == OutputDirectoryMode.ALWAYS)?
						p.suffix("/" + getDirectoryFileName(taskNumber)) : p;

		this.stream = null;
		logger.info("TaskNumber {} creating output file: {}", taskNumber, this.actualFilePath);

		// create output file
		if (fs.isDistributedFS() && fs instanceof SafetyNetWrapperFileSystem) {
			FileSystem wFS = ((SafetyNetWrapperFileSystem) fs).getWrappedDelegate();

			if (wFS instanceof HadoopFileSystem) {
				HadoopFileSystem dFS = ((HadoopFileSystem) wFS);
				org.apache.hadoop.conf.Configuration hadoopConfig = dFS.getHadoopFileSystem().getConf();
				long blockSize = hadoopConfig.getLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
				int bufferSize = hadoopConfig.getInt(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY, CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);
				logger.info("HDFS options: bufferSize {}, blockSize {}, replication {}", bufferSize, blockSize, replication);

				this.stream = dFS.create(this.actualFilePath, true,	bufferSize, replication, blockSize);
			}
		}

		if (this.stream == null)
			this.stream = fs.create(this.actualFilePath, getWriteMode());

		// at this point, the file creation must have succeeded, or an exception has been thrown
		this.fileCreated = true;

		this.wrt = this.charsetName == null?
				new OutputStreamWriter(new BufferedOutputStream(this.stream, 4096))
				: new OutputStreamWriter(new BufferedOutputStream(this.stream, 4096), this.charsetName);
	}

	@Override
	public void writeRecord(Tuple2<Kmer,Integer> kmer) throws IOException {
		this.wrt.write(kmer.f0.toString());
		this.wrt.write(DEFAULT_FIELD_DELIMITER);
		this.wrt.write(kmer.f1.toString());
		this.wrt.write(DEFAULT_LINE_DELIMITER);	
	}

	@Override
	public String toString() {
		return "KmerCsvOF (" + getOutputFilePath() + ")";
	}

	@Override
	public void close() throws IOException {
		if (wrt != null) {
			this.wrt.flush();
			this.wrt.close();
			this.wrt = null;
		}

		try {
			super.close();
		} catch (IOException e) {
			logger.warn("Could not properly close KmerCsvOutputFormat", e);
			logger.warn("Retrying");
			super.close();
		}
	}

	@Override
	public void tryCleanupOnError() {
		if (this.fileCreated) {
			this.fileCreated = false;

			try {
				close();
			} catch (IOException e) {
				logger.warn("Could not properly close KmerCsvOutputFormat", e);
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

	/**
	 * The purpose of this method is solely to check whether the data type to be processed is in
	 * fact a tuple type.
	 */
	@Override
	public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
		if (!type.isTupleType()) {
			throw new InvalidProgramException(
					"The " + KmerCsvOutputFormat.class.getSimpleName()
					+ " can only be used to write tuple data sets.");
		}
	}
}
