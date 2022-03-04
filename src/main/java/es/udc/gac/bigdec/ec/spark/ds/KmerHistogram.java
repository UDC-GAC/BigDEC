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
package es.udc.gac.bigdec.ec.spark.ds;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.function.MapPartitionsFunction;

import es.udc.gac.bigdec.kmer.Kmer;
import scala.Tuple2;

public class KmerHistogram implements MapPartitionsFunction<Tuple2<Kmer,Object>,int[]>{

	private static final long serialVersionUID = 4963350595288689056L;

	private int histogramSize;

	public KmerHistogram(int histogramSize) {
		this.histogramSize = histogramSize;
	}

	@Override
	public Iterator<int[]> call(Iterator<Tuple2<Kmer,Object>> kmers) throws Exception {
		int[] histogram = new int[histogramSize];
		int maxCounter = histogramSize - 1;
		int counter;

		while(kmers.hasNext()) {
			counter = ((Long) kmers.next()._2()).intValue();
			histogram[(counter >= maxCounter)? maxCounter : counter]++;
		}

		return Arrays.asList(histogram).iterator();
	}
}
