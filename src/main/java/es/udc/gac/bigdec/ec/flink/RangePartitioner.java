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

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.hadoop.io.LongWritable;

public class RangePartitioner implements Partitioner<LongWritable> {

	private static final long serialVersionUID = -401498768038251208L;

	private long partitionSize;

	public RangePartitioner(long maxRange, int numPartitions) {
		this.partitionSize = (int) Math.ceil(((double) maxRange) / numPartitions);
	}

	public long getPartitionSize() {
		return partitionSize;
	}

	@Override
	public int partition(LongWritable key, int numPartitions) {
		return (int) (key.get() / partitionSize);
	}
}
