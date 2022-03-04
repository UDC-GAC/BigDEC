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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.LongWritable;

import es.udc.gac.bigdec.sequence.Sequence;

public class QsHistogramPaired implements MapPartitionFunction<Tuple3<LongWritable,Sequence,Sequence>,int[]>{

	private static final long serialVersionUID = -6369368883606389651L;

	private int histogramSize;

	public QsHistogramPaired(int histogramSize) {
		this.histogramSize = histogramSize;
	}

	@Override
	public void mapPartition(Iterable<Tuple3<LongWritable,Sequence,Sequence>> pairedSequences, Collector<int[]> out) throws Exception {
		int[] histogram = new int[histogramSize];
		byte[] qualsLeft,qualsRight;
		int i;

		for(Tuple3<LongWritable,Sequence,Sequence> pairedSeq: pairedSequences) {
			qualsLeft = pairedSeq.f1.getQuals();
			qualsRight = pairedSeq.f2.getQuals();

			for (i=0; i<pairedSeq.f1.getLength();i++) {
				histogram[qualsLeft[i]]++;
				histogram[qualsRight[i]]++;
			}
		}

		out.collect(histogram);
	}
}
