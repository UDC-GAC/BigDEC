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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.hadoop.io.LongWritable;

import es.udc.gac.bigdec.sequence.Sequence;

@ForwardedFields("f0;f1;f2")
public class QsHistogramPaired extends RichMapFunction<Tuple3<LongWritable,Sequence,Sequence>,Tuple3<LongWritable,Sequence,Sequence>> {

	private static final long serialVersionUID = -1115364744722279422L;

	private Histogram histogram;
	private String histogramName;

	public QsHistogramPaired(String histogramName) {
		this.histogram = new Histogram();
		this.histogramName = histogramName;
	}

	@Override
	public void open(org.apache.flink.configuration.Configuration config) throws Exception {
		getRuntimeContext().addAccumulator(histogramName, histogram);
	}

	@Override
	public Tuple3<LongWritable,Sequence,Sequence> map(Tuple3<LongWritable,Sequence,Sequence> seq) throws Exception {
		byte[] qualsLeft = seq.f1.getQuals();
		byte[] qualsRight = seq.f2.getQuals();

		for (int i=0; i<qualsLeft.length;i++) {
			this.histogram.add((int) qualsLeft[i]);
			this.histogram.add((int) qualsRight[i]);
		}

		return seq;
	}
}
