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

import es.udc.gac.bigdec.kmer.Kmer;

public class KmerHistogram implements MapPartitionFunction<Tuple2<Kmer,Integer>,int[]>{

	private static final long serialVersionUID = -775417753327172599L;

	private int histogramSize;

	public KmerHistogram(int histogramSize) {
		this.histogramSize = histogramSize;
	}

	@Override
	public void mapPartition(Iterable<Tuple2<Kmer, Integer>> kmers, Collector<int[]> out) throws Exception {
		int[] histogram = new int[histogramSize];
		int maxCounter = histogramSize - 1;

		for(Tuple2<Kmer, Integer> kmer: kmers)
			histogram[(kmer.f1 >= maxCounter)? maxCounter : kmer.f1]++;

		out.collect(histogram);
	}
}
