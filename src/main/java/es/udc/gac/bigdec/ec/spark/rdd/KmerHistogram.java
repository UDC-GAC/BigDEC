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
package es.udc.gac.bigdec.ec.spark.rdd;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;

import es.udc.gac.bigdec.kmer.KmerKey;
import scala.Tuple2;

public class KmerHistogram implements FlatMapFunction<Iterator<Tuple2<KmerKey,Short>>,int[]>{

	private static final long serialVersionUID = -939790648762633924L;

	private int histogramSize;

	public KmerHistogram(int histogramSize) {
		this.histogramSize = histogramSize;
	}

	@Override
	public Iterator<int[]> call(Iterator<Tuple2<KmerKey,Short>> kmers) throws Exception {
		int[] histogram = new int[histogramSize];

		while(kmers.hasNext())
			histogram[kmers.next()._2]++;

		return Arrays.asList(histogram).iterator();
	}
}
