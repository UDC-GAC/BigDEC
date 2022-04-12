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

import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.io.LongWritable;

import es.udc.gac.bigdec.sequence.Sequence;

@ForwardedFields("f0;f1")
public class QsHistogramSingle extends RichMapFunction<Tuple2<LongWritable,Sequence>,Tuple2<LongWritable,Sequence>> {

	private static final long serialVersionUID = 6697631266487890133L;

	private Histogram histogram;
	private String histogramName;

	public QsHistogramSingle(String histogramName) {
		this.histogram = new Histogram();
		this.histogramName = histogramName;
	}

	@Override
	public void open(org.apache.flink.configuration.Configuration config) throws Exception {
		getRuntimeContext().addAccumulator(histogramName, histogram);
	}

	@Override
	public Tuple2<LongWritable,Sequence> map(Tuple2<LongWritable,Sequence> seq) throws Exception {
		byte[] quals = seq.f1.getQuals();

		for (int i=0; i<quals.length;i++) {
			this.histogram.add((int) quals[i]);
		}

		return seq;
	}
}
