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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormatBase;
import org.apache.flink.api.java.hadoop.mapreduce.wrapper.HadoopInputSplit;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

public class HadoopInputFormat<K, V> extends HadoopInputFormatBase<K, V, Tuple2<K, V>> implements ResultTypeQueryable<Tuple2<K, V>> {

	private static final long serialVersionUID = -4437057064551067927L;

	private org.apache.hadoop.mapreduce.InputFormat<K, V> mapreduceInputFormat;

	public HadoopInputFormat(
			org.apache.hadoop.mapreduce.InputFormat<K, V> mapreduceInputFormat,
			Class<K> key,
			Class<V> value,
			Job job) {
		super(mapreduceInputFormat, key, value, job);
		this.mapreduceInputFormat = mapreduceInputFormat;
	}

	public HadoopInputFormat(
			org.apache.hadoop.mapreduce.InputFormat<K, V> mapreduceInputFormat,
			Class<K> key,
			Class<V> value)
					throws IOException {
		super(mapreduceInputFormat, key, value, Job.getInstance());
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

	@Override
	public HadoopInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		JobContext jobContext = new JobContextImpl(getConfiguration(), new JobID());

		jobContext.getCredentials().addAll(this.credentials);
		Credentials currentUserCreds = getCredentialsFromUGI(UserGroupInformation.getCurrentUser());
		if (currentUserCreds != null) {
			jobContext.getCredentials().addAll(currentUserCreds);
		}

		List<org.apache.hadoop.mapreduce.InputSplit> splits;
		try {
			splits = this.mapreduceInputFormat.getSplits(jobContext);
		} catch (InterruptedException e) {
			throw new IOException("Could not get Splits.", e);
		}
		HadoopInputSplit[] hadoopInputSplits = new HadoopInputSplit[splits.size()];

		for (int i = 0; i < hadoopInputSplits.length; i++) {
			hadoopInputSplits[i] = new HadoopInputSplit(i, splits.get(i), jobContext);
		}
		return hadoopInputSplits;
	}

	private void writeObject(ObjectOutputStream out) throws IOException {
		super.write(out);
		out.writeUTF(this.mapreduceInputFormat.getClass().getName());
	}

	@SuppressWarnings({ "unchecked" })
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		super.read(in);
		String hadoopInputFormatClassName = in.readUTF();

		try {
			this.mapreduceInputFormat =
					(org.apache.hadoop.mapreduce.InputFormat<K, V>)
					Class.forName(
							hadoopInputFormatClassName,
							true,
							Thread.currentThread().getContextClassLoader())
					.getDeclaredConstructor().newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Unable to instantiate the Hadoop input format", e);
		}
	}
}
