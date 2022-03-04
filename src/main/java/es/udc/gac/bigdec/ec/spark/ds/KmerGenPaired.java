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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import es.udc.gac.bigdec.kmer.Kmer;
import es.udc.gac.bigdec.kmer.KmerGenerator;
import es.udc.gac.bigdec.sequence.Sequence;
import scala.Tuple2;

public class KmerGenPaired implements FlatMapFunction<Tuple2<Sequence,Sequence>, Tuple2<Kmer,Short>> {

	private static final long serialVersionUID = 7214455086525563480L;

	private byte kmerLength;
	private boolean ignoreNBases;

	public KmerGenPaired(byte kmerLength, boolean ignoreNBases) {
		this.kmerLength = kmerLength;
		this.ignoreNBases = ignoreNBases;
	}

	@Override
	public Iterator<Tuple2<Kmer,Short>> call(Tuple2<Sequence,Sequence> pairedSequence) {
		int nkmers = ((pairedSequence._1.getLength() - kmerLength) + 1) * 2;
		List<Tuple2<Kmer,Short>> kmers = new ArrayList<Tuple2<Kmer,Short>>(nkmers);
		Kmer kmer = KmerGenerator.createKmer();
		Kmer kmerRC = KmerGenerator.createKmer();

		// Generate all k-mers for the left read
		KmerGenerator.generateSparkKmers(pairedSequence._1, kmer, kmerRC, kmerLength, ignoreNBases, kmers);
		// Generate all k-mers for the right read
		KmerGenerator.generateSparkKmers(pairedSequence._2, kmer, kmerRC, kmerLength, ignoreNBases, kmers);

		return kmers.iterator();
	}
}
