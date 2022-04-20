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

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.hadoop.mapreduce.utils.HadoopUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.udc.gac.hadoop.sequence.parser.mapreduce.PairedEndInputSplit;
import es.udc.gac.hadoop.sequence.parser.mapreduce.PairedEndSequenceInputFormat;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

public class HadoopFileInputFormat<K, V> extends FileInputFormat<Tuple2<K, V>> implements ResultTypeQueryable<Tuple2<K, V>> {

	private static final long serialVersionUID = 3942040739031623174L;
	private static final Logger logger = LoggerFactory.getLogger(HadoopFileInputFormat.class);

	// Mutexes to avoid concurrent operations on Hadoop InputFormats.
	// Hadoop parallelizes tasks across JVMs which is why they might rely on this JVM isolation.
	// In contrast, Flink parallelizes using Threads, so multiple Hadoop InputFormat instances
	// might be used in the same JVM.
	private static final Object OPEN_MUTEX = new Object();
	private static final Object CONFIGURE_MUTEX = new Object();
	private static final Object CLOSE_MUTEX = new Object();

	// NOTE: this class is using a custom serialization logic, without a defaultWriteObject()
	// method.
	// Hence, all fields here are "transient".
	private org.apache.hadoop.mapreduce.lib.input.FileInputFormat<K, V> mapreduceFileInputFormat;
	protected Class<K> keyClass;
	protected Class<V> valueClass;
	private org.apache.hadoop.conf.Configuration configuration;

	protected transient Credentials credentials;
	protected transient RecordReader<K, V> recordReader;
	private transient float avgRecordBytes;
	protected boolean fetched = false;
	protected boolean hasNext;

	public HadoopFileInputFormat(
			org.apache.hadoop.mapreduce.lib.input.FileInputFormat<K, V> mapreduceFileInputFormat,
			Class<K> key,
			Class<V> value,
			Job job) {
		Preconditions.checkNotNull(job, "Job can not be null");
		this.credentials = job.getCredentials();
		this.mapreduceFileInputFormat = Preconditions.checkNotNull(mapreduceFileInputFormat);
		this.keyClass = Preconditions.checkNotNull(key);
		this.valueClass = Preconditions.checkNotNull(value);
		this.configuration = job.getConfiguration();
		this.avgRecordBytes = BaseStatistics.AVG_RECORD_BYTES_UNKNOWN;
		HadoopUtils.mergeHadoopConf(configuration);
	}

	public void setAvgRecordBytes(float avgRecordBytes) {
		this.avgRecordBytes = avgRecordBytes;
	}

	@Override
	public void configure(Configuration parameters) {

		// enforce sequential configuration() calls
		synchronized (CONFIGURE_MUTEX) {
			if (mapreduceFileInputFormat instanceof Configurable) {
				((Configurable) mapreduceFileInputFormat).setConf(configuration);
			}
		}
	}

	@Override
	public FileBaseStatistics getStatistics(BaseStatistics cachedStats) throws IOException {
		JobContext jobContext = new JobContextImpl(configuration, null);

		final FileBaseStatistics cachedFileStats =
				(cachedStats instanceof FileBaseStatistics)
				? (FileBaseStatistics) cachedStats : null;

		try {
			final org.apache.hadoop.fs.Path[] paths;

			if (this.mapreduceFileInputFormat instanceof PairedEndSequenceInputFormat) {
				paths = PairedEndSequenceInputFormat.getInputPaths(jobContext);
			} else {
				paths = org.apache.hadoop.mapreduce.lib.input.FileInputFormat.getInputPaths(jobContext);
			}

			return getFileStats(cachedFileStats, paths, new ArrayList<FileStatus>(1));
		} catch (IOException ioex) {
			if (logger.isWarnEnabled()) {
				logger.warn("Could not determine statistics due to an I/O error: " + ioex.getMessage());
			}
		} catch (Throwable t) {
			if (logger.isErrorEnabled()) {
				logger.error("Unexpected problem while getting the file statistics: " + t.getMessage(), t);
			}
		}

		// no statistics available
		return null;
	}

	@Override
	public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		JobContext jobContext = new JobContextImpl(configuration, new JobID());

		jobContext.getCredentials().addAll(this.credentials);
		Credentials currentUserCreds = getCredentialsFromUGI(UserGroupInformation.getCurrentUser());
		if (currentUserCreds != null) {
			jobContext.getCredentials().addAll(currentUserCreds);
		}

		List<org.apache.hadoop.mapreduce.InputSplit> hadoopSplits = this.mapreduceFileInputFormat.getSplits(jobContext);
		FileInputSplit[] splits = new FileInputSplit[hadoopSplits.size()];
		org.apache.hadoop.mapreduce.lib.input.FileSplit fileSplit;
		long length;

		logger.info("splits {} (minNumSplits {})", hadoopSplits.size(), minNumSplits);

		for (int i = 0; i < splits.length; i++) {
			if (!(hadoopSplits.get(i) instanceof org.apache.hadoop.mapreduce.lib.input.FileSplit)) {
				throw new IllegalArgumentException("InputSplit must implement org.apache.hadoop.mapreduce.lib.input.FileSplit.");
			}

			fileSplit = (org.apache.hadoop.mapreduce.lib.input.FileSplit) hadoopSplits.get(i);
			length = fileSplit.getLength();

			if (hadoopSplits.get(i) instanceof PairedEndInputSplit) {
				length = ((PairedEndInputSplit) hadoopSplits.get(i)).getLength(0);
			}

			splits[i] = new FileInputSplit(i, new Path(fileSplit.getPath().toString()), 
					fileSplit.getStart(), length, fileSplit.getLocations());

			logger.info("split {}: {} ({})", i, fileSplit, fileSplit.getLocations());
		}

		return splits;
	}

	@Override
	public void open(FileInputSplit split) throws IOException {

		// enforce sequential open() calls
		synchronized (OPEN_MUTEX) {
			TaskAttemptContext context =
					new TaskAttemptContextImpl(configuration, new TaskAttemptID());

			try {
				org.apache.hadoop.mapreduce.lib.input.FileSplit hadoopSplit;

				if (this.mapreduceFileInputFormat instanceof PairedEndSequenceInputFormat) {
					logger.info("Opening paired split {} ({})", split, split.getHostnames());

					org.apache.hadoop.fs.Path[] hadoopPaths = PairedEndSequenceInputFormat.getInputPaths(context);
					hadoopSplit = new PairedEndInputSplit();
					org.apache.hadoop.mapreduce.lib.input.FileSplit leftSplit;
					org.apache.hadoop.mapreduce.lib.input.FileSplit rightSplit;
					leftSplit = new org.apache.hadoop.mapreduce.lib.input.FileSplit(hadoopPaths[0], split.getStart(), 
							split.getLength(), split.getHostnames());	
					rightSplit = new org.apache.hadoop.mapreduce.lib.input.FileSplit(hadoopPaths[1], split.getStart(), 
							split.getLength(), split.getHostnames());	
					((PairedEndInputSplit)hadoopSplit).add(leftSplit);
					((PairedEndInputSplit)hadoopSplit).add(rightSplit);
				} else {
					logger.info("Opening split {} ({})", split, split.getHostnames());

					org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(split.getPath().toString());
					hadoopSplit = new org.apache.hadoop.mapreduce.lib.input.FileSplit(hadoopPath, split.getStart(), 
							split.getLength(), split.getHostnames());				
				}

				this.recordReader = this.mapreduceFileInputFormat.createRecordReader(hadoopSplit, context);
				this.recordReader.initialize(hadoopSplit, context);
			} catch (InterruptedException e) {
				throw new IOException("Could not create RecordReader.", e);
			} finally {
				this.fetched = false;
			}
		}
	}

	@Override
	public boolean reachedEnd() throws IOException {
		if (!this.fetched) {
			fetchNext();
		}
		return !this.hasNext;
	}

	protected void fetchNext() throws IOException {
		try {
			this.hasNext = this.recordReader.nextKeyValue();
		} catch (InterruptedException e) {
			throw new IOException("Could not fetch next KeyValue pair.", e);
		} finally {
			this.fetched = true;
		}
	}

	@Override
	public void close() throws IOException {
		if (this.recordReader != null) {
			// enforce sequential close() calls
			synchronized (CLOSE_MUTEX) {
				this.recordReader.close();
			}
		}
	}

	@Override
	public Tuple2<K, V> nextRecord(Tuple2<K, V> record) throws IOException {
		if (!this.fetched) {
			fetchNext();
		}
		if (!this.hasNext) {
			return null;
		}
		try {
			record.f0 = recordReader.getCurrentKey();
			record.f1 = recordReader.getCurrentValue();
		} catch (InterruptedException e) {
			throw new IOException("Could not get KeyValue pair.", e);
		}
		this.fetched = false;

		return record;
	}

	@Override
	public TypeInformation<Tuple2<K, V>> getProducedType() {
		return new TupleTypeInfo<Tuple2<K, V>>(
				TypeExtractor.createTypeInfo(keyClass), TypeExtractor.createTypeInfo(valueClass));
	}

	// --------------------------------------------------------------------------------------------
	//  Helper methods
	// --------------------------------------------------------------------------------------------

	private FileBaseStatistics getFileStats(
			FileBaseStatistics cachedStats,
			org.apache.hadoop.fs.Path[] hadoopFilePaths,
			ArrayList<FileStatus> files) throws IOException {

		long latestModTime = 0L;

		// get the file info and check whether the cached statistics are still valid.
		for (org.apache.hadoop.fs.Path hadoopPath : hadoopFilePaths) {
			final Path filePath = new Path(hadoopPath.toUri());
			final FileSystem fs = FileSystem.get(filePath.toUri());

			final FileStatus file = fs.getFileStatus(filePath);
			latestModTime = Math.max(latestModTime, file.getModificationTime());

			// enumerate all files and check their modification time stamp.
			if (file.isDir()) {
				FileStatus[] fss = fs.listStatus(filePath);
				files.ensureCapacity(files.size() + fss.length);

				for (FileStatus s : fss) {
					if (!s.isDir()) {
						files.add(s);
						latestModTime = Math.max(s.getModificationTime(), latestModTime);
					}
				}
			} else {
				files.add(file);
			}
		}

		// check whether the cached statistics are still valid, if we have any
		if (cachedStats != null && latestModTime <= cachedStats.getLastModificationTime()) {
			return cachedStats;
		}

		// calculate the whole length
		long len = 0;
		for (FileStatus s : files) {
			len += s.getLen();
		}

		if (this.mapreduceFileInputFormat instanceof PairedEndSequenceInputFormat)
			len = files.get(0).getLen();

		logger.info("Length {} bytes, Avg record bytes {}", len, avgRecordBytes);

		// sanity check
		if (len <= 0) {
			len = BaseStatistics.SIZE_UNKNOWN;
		}

		return new FileBaseStatistics(latestModTime, len, avgRecordBytes);
	}

	// --------------------------------------------------------------------------------------------
	//  Custom serialization methods
	// --------------------------------------------------------------------------------------------

	private void writeObject(ObjectOutputStream out) throws IOException {
		this.credentials.write(out);
		out.writeUTF(this.mapreduceFileInputFormat.getClass().getName());
		out.writeUTF(this.keyClass.getName());
		out.writeUTF(this.valueClass.getName());
		this.configuration.write(out);
	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		this.credentials = new Credentials();
		credentials.readFields(in);
		String hadoopInputFormatClassName = in.readUTF();
		String keyClassName = in.readUTF();
		String valueClassName = in.readUTF();

		org.apache.hadoop.conf.Configuration configuration =
				new org.apache.hadoop.conf.Configuration();
		configuration.readFields(in);

		if (this.configuration == null) {
			this.configuration = configuration;
		}

		try {
			this.mapreduceFileInputFormat =
					(org.apache.hadoop.mapreduce.lib.input.FileInputFormat<K, V>)
					Class.forName(
							hadoopInputFormatClassName,
							true,
							Thread.currentThread().getContextClassLoader()).getDeclaredConstructor().newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Unable to instantiate the hadoop input format", e);
		}
		try {
			this.keyClass =
					(Class<K>)
					Class.forName(
							keyClassName,
							true,
							Thread.currentThread().getContextClassLoader());
		} catch (Exception e) {
			throw new RuntimeException("Unable to find key class.", e);
		}
		try {
			this.valueClass =
					(Class<V>)
					Class.forName(
							valueClassName,
							true,
							Thread.currentThread().getContextClassLoader());
		} catch (Exception e) {
			throw new RuntimeException("Unable to find value class.", e);
		}
	}

	/**
	 * @param ugi The user information
	 * @return new credentials object from the user information.
	 */
	public static Credentials getCredentialsFromUGI(UserGroupInformation ugi) {
		return ugi.getCredentials();
	}
}
