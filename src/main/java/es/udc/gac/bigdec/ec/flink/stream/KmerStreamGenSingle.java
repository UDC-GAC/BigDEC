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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.LongWritable;

import es.udc.gac.bigdec.kmer.Kmer;
import es.udc.gac.bigdec.kmer.KmerGenerator;
import es.udc.gac.bigdec.sequence.Sequence;

@ReadFields("f1.*")
public class KmerStreamGenSingle implements FlatMapFunction<Tuple2<LongWritable,Sequence>, Kmer> {

	private static final long serialVersionUID = -8748701805643750129L;

	private byte kmerLength;
	private boolean ignoreNBases;

	public KmerStreamGenSingle(byte kmerLength, boolean ignoreNBases) {
		this.kmerLength = kmerLength;
		this.ignoreNBases = ignoreNBases;
	}

	@Override
	public void flatMap(Tuple2<LongWritable,Sequence> sequence, Collector<Kmer> out) throws Exception {
		Kmer kmer = KmerGenerator.createKmer();
		Kmer kmerRC = KmerGenerator.createKmer();

		// Generate all k-mers for this read
		KmerGenerator.generateFlinkKmers(sequence.f1, kmer, kmerRC, kmerLength, ignoreNBases, out);
	}
}
