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
package es.udc.gac.bigdec.ec.flink.ds;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.LongWritable;

import es.udc.gac.bigdec.sequence.Sequence;

public class QsHistogramSingle implements MapPartitionFunction<Tuple2<LongWritable,Sequence>,int[]>{

	private static final long serialVersionUID = -775417753327172599L;

	private int histogramSize;

	public QsHistogramSingle(int histogramSize) {
		this.histogramSize = histogramSize;
	}

	@Override
	public void mapPartition(Iterable<Tuple2<LongWritable,Sequence>> sequences, Collector<int[]> out) throws Exception {
		int[] histogram = new int[histogramSize];
		byte[] quals;
		int i;

		for(Tuple2<LongWritable,Sequence> seq: sequences) {
			quals = seq.f1.getQuals();

			for (i=0; i<seq.f1.getLength();i++)
				histogram[quals[i]]++;
		}

		out.collect(histogram);
	}
}
