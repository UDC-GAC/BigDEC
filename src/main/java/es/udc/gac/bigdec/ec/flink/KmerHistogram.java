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

import es.udc.gac.bigdec.kmer.Kmer;

@ForwardedFields("f0;f1")
public class KmerHistogram extends RichMapFunction<Tuple2<Kmer,Integer>,Tuple2<Kmer,Integer>> {

	private static final long serialVersionUID = -775417753327172599L;

	private Histogram histogram;
	private String histogramName;
	private int maxCounter;

	public KmerHistogram(int histogramSize, String histogramName) {
		this.maxCounter = histogramSize - 1;
		this.histogram = new Histogram();
		this.histogramName = histogramName;
	}

	@Override
	public void open(org.apache.flink.configuration.Configuration config) throws Exception {
		getRuntimeContext().addAccumulator(histogramName, histogram);
	}

	@Override
	public Tuple2<Kmer,Integer> map(Tuple2<Kmer, Integer> kmer) throws Exception {
		this.histogram.add((kmer.f1 >= maxCounter)? maxCounter : kmer.f1);
		return kmer;
	}
}
