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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import es.udc.gac.bigdec.kmer.Kmer;
import es.udc.gac.bigdec.kmer.KmerKey;
import es.udc.gac.bigdec.kmer.KmerGenerator;
import es.udc.gac.bigdec.sequence.Sequence;
import scala.Tuple2;

public class KmerGenSingle implements PairFlatMapFunction<Sequence, KmerKey, Short> {

	private static final long serialVersionUID = -8810510778920275591L;

	private byte kmerLength;
	private boolean ignoreNBases;

	public KmerGenSingle(byte kmerLength, boolean ignoreNBases) {
		this.kmerLength = kmerLength;
		this.ignoreNBases = ignoreNBases;
	}

	@Override
	public Iterator<Tuple2<KmerKey,Short>> call(Sequence sequence) {
		List<Tuple2<KmerKey,Short>> kmers = new ArrayList<Tuple2<KmerKey,Short>>((sequence.getLength() - kmerLength) + 1);
		Kmer kmer = KmerGenerator.createKmer();
		Kmer kmerRC = KmerGenerator.createKmer();

		// Generate all k-mers for this read
		KmerGenerator.generateSparkKmerKeys(sequence, kmer, kmerRC, kmerLength, ignoreNBases, kmers);

		return kmers.iterator();
	}
}
