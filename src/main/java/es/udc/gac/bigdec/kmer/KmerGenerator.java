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
package es.udc.gac.bigdec.kmer;

import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import es.udc.gac.bigdec.sequence.Sequence;

public final class KmerGenerator {

	public static final byte MIN_KMER_SIZE = 2;
	public static final byte MAX_KMER_SIZE = 32;
	private static final short ONE = Short.valueOf("1");

	private KmerGenerator() {}

	public static Kmer createKmer() {
		return new Kmer();
	}

	public static Kmer createKmer(Long bases) {
		return new Kmer(bases);
	}

	public static Kmer createKmer(Kmer kmer) {
		return new Kmer(kmer.getBases());
	}

	public static KmerKey createKmerKey() {
		return new KmerKey();
	}

	public static KmerKey createKmerKey(Long bases) {
		return new KmerKey(bases);
	}

	private static Kmer getCanonicalKmer(byte[] bases, int pos, byte kmerLength, Kmer kmer, Kmer kmerRC) {
		kmer.forwardBase(bases[pos], kmerLength);
		kmerRC.forwardBaseRC(bases[pos], kmerLength);
		// Return the canonical k-mer
		return (kmer.lower(kmerRC))? kmer : kmerRC;
	}

	public static void generateSparkKmers(Sequence sequence, Kmer kmer, Kmer kmerRC, byte kmerLength, boolean ignoreNBases,
			List<scala.Tuple2<Kmer,Short>> listOfKmers) {

		byte[] bases = sequence.getBases();
		int i = 0;
		Kmer newKmer;
		final int kLen = kmerLength - 1;

		while (i + kLen < bases.length) {
			if (ignoreNBases) {
				kmer.set(bases, i, kmerLength);
			} else if (!kmer.load(bases, i, kmerLength)) {
				// k-mer contains 'N' bases
				i++;
				continue;
			}

			// Build reverse complement representation
			kmerRC.setRC(bases, i, kmerLength);

			// Insert the canonical k-mer
			if (kmer.lower(kmerRC))
				newKmer = kmer;
			else
				newKmer = kmerRC;

			listOfKmers.add(new scala.Tuple2<Kmer,Short>(KmerGenerator.createKmer(newKmer), ONE));
			i += kmerLength;

			// Generate remaining k-mers for this read
			while (i < bases.length) {
				if (!ignoreNBases && Kmer.isNBase(bases[i])) {
					i++;
					break;
				}
				newKmer = KmerGenerator.getCanonicalKmer(bases, i, kmerLength, kmer, kmerRC);
				listOfKmers.add(new scala.Tuple2<Kmer,Short>(KmerGenerator.createKmer(newKmer), ONE));
				i++;
			}
		}
	}

	public static void generateSparkKmerKeys(Sequence sequence, Kmer kmer, Kmer kmerRC, byte kmerLength, boolean ignoreNBases,
			List<scala.Tuple2<KmerKey,Short>> listOfKmers) {

		byte[] bases = sequence.getBases();
		int i = 0;
		long newKmer;
		final int kLen = kmerLength - 1;

		while (i + kLen < bases.length) {
			if (ignoreNBases) {
				kmer.set(bases, i, kmerLength);
			} else if (!kmer.load(bases, i, kmerLength)) {
				// k-mer contains 'N' bases
				i++;
				continue;
			}

			// Build reverse complement representation
			kmerRC.setRC(bases, i, kmerLength);

			// Insert the canonical k-mer
			if (kmer.lower(kmerRC))
				newKmer = kmer.getBases();
			else
				newKmer = kmerRC.getBases();

			listOfKmers.add(new scala.Tuple2<KmerKey,Short>(KmerGenerator.createKmerKey(newKmer), ONE));
			i += kmerLength;

			// Generate remaining k-mers for this read
			while (i < bases.length) {
				if (!ignoreNBases && Kmer.isNBase(bases[i])) {
					i++;
					break;
				}
				newKmer = KmerGenerator.getCanonicalKmer(bases, i, kmerLength, kmer, kmerRC).getBases();
				listOfKmers.add(new scala.Tuple2<KmerKey,Short>(KmerGenerator.createKmerKey(newKmer), ONE));
				i++;
			}
		}
	}

	public static void generateFlinkKmers(Sequence sequence, Kmer kmer, Kmer kmerRC, byte kmerLength,
			Tuple2<Kmer,Integer> tuple2, boolean ignoreNBases, Collector<Tuple2<Kmer,Integer>> listOfKmers) {

		byte[] bases = sequence.getBases();
		int i = 0;
		final int kLen = kmerLength - 1;

		while (i + kLen < bases.length) {
			if (ignoreNBases) {
				kmer.set(bases, i, kmerLength);
			} else if (!kmer.load(bases, i, kmerLength)) {
				// k-mer contains 'N' bases
				i++;
				continue;
			}

			// Build reverse complement representation
			kmerRC.setRC(bases, i, kmerLength);

			// Insert the canonical k-mer
			if (kmer.lower(kmerRC))
				tuple2.setField(kmer, 0);
			else
				tuple2.setField(kmerRC, 0);

			listOfKmers.collect(tuple2);
			i += kmerLength;

			// Generate remaining k-mers for this read
			while (i < bases.length) {
				if (!ignoreNBases && Kmer.isNBase(bases[i])) {
					i++;
					break;
				}
				tuple2.setField(KmerGenerator.getCanonicalKmer(bases, i, kmerLength, kmer, kmerRC), 0);
				listOfKmers.collect(tuple2);
				i++;
			}
		}
	}
}
