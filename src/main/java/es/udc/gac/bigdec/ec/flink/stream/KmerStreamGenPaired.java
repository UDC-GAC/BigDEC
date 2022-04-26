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
package es.udc.gac.bigdec.ec.flink.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation.ReadFields;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.LongWritable;

import es.udc.gac.bigdec.kmer.Kmer;
import es.udc.gac.bigdec.kmer.KmerGenerator;
import es.udc.gac.bigdec.sequence.Sequence;

@ReadFields("f1.*;f2.*")
public class KmerStreamGenPaired implements FlatMapFunction<Tuple3<LongWritable,Sequence,Sequence>, Kmer> {

	private static final long serialVersionUID = -7335452060418754060L;

	private byte kmerLength;
	private boolean ignoreNBases;

	public KmerStreamGenPaired(byte kmerLength, boolean ignoreNBases) {
		this.kmerLength = kmerLength;
		this.ignoreNBases = ignoreNBases;
	}

	@Override
	public void flatMap(Tuple3<LongWritable,Sequence,Sequence> pairedSequence, Collector<Kmer> out) throws Exception {
		Kmer kmer = KmerGenerator.createKmer();
		Kmer kmerRC = KmerGenerator.createKmer();

		// Generate all k-mers for the left read
		KmerGenerator.generateFlinkKmers(pairedSequence.f1, kmer, kmerRC, kmerLength, ignoreNBases, out);
		// Generate all k-mers for the right read
		KmerGenerator.generateFlinkKmers(pairedSequence.f2, kmer, kmerRC, kmerLength, ignoreNBases, out);
	}
}