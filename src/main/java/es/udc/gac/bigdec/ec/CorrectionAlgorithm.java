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
package es.udc.gac.bigdec.ec;

import java.io.Serializable;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.udc.gac.bigdec.kmer.Kmer;
import es.udc.gac.bigdec.sequence.Sequence;

public abstract class CorrectionAlgorithm implements Serializable {

	private static final long serialVersionUID = -1778098793705259196L;
	private static final Logger logger = LoggerFactory.getLogger(CorrectionAlgorithm.class);

	private byte KMER_LENGTH;
	private short KMER_THRESHOLD;
	private transient Map<Kmer,Short> solidKmersMap;
	private int numberOfAlgorithms;
	private int numberOfSolidKmers;
	private String outputPath1;
	private String outputPath2;

	public CorrectionAlgorithm(byte kmerLength, int numberOfAlgorithms) {
		this.KMER_LENGTH = kmerLength;
		this.solidKmersMap = null;
		this.numberOfAlgorithms = numberOfAlgorithms;
		this.numberOfSolidKmers = 0;
	}

	public byte getKmerLength() {
		return KMER_LENGTH;
	}

	public short getKmerThreshold() {
		return KMER_THRESHOLD;
	}

	public int getNumberOfAlgorithms() {
		return numberOfAlgorithms;
	}

	public int getNumberOfSolidKmers() {
		return numberOfSolidKmers;
	}

	public void setKmerThreshold(int kmerThreshold) {
		this.KMER_THRESHOLD = (short) kmerThreshold;
	}

	public void setSolidKmers(Map<Kmer,Short> solidKmersMap) {
		this.solidKmersMap = solidKmersMap;
		logger.trace("{}: {} k-mers", this.toString(), solidKmersMap.size());
	}

	public void setNumberOfSolidKmers(int numberOfSolidKmers) {
		this.numberOfSolidKmers = numberOfSolidKmers;
	}

	public Path getOutputPath1() {
		return new Path(outputPath1);
	}

	public Path getOutputPath2() {
		return new Path(outputPath2);
	}

	public void setOutputPath1(String outputPath1) {
		this.outputPath1 = outputPath1;
	}

	public void setOutputPath2(String outputPath2) {
		this.outputPath2 = outputPath2;
	}

	public int getKmerMultiplicity(Kmer kmer, Kmer kmerRC) {
		Short value = solidKmersMap.get(kmer);

		if (value != null)
			return value;

		kmerRC.toRC(kmer, KMER_LENGTH);
		value = solidKmersMap.get(kmerRC);

		return (value == null)? 0 : value;
	}

	public int getKmerMultiplicity(Kmer kmer) {
		Short value = solidKmersMap.get(kmer);
		return (value == null)? 0 : value;
	}

	public static int replaceNs(Sequence sequence, byte substBase) {
		int numberOfNs = 0;
		byte[] bases = sequence.getBases();

		for (int i=0; i<sequence.getLength(); i++) {
			if (Kmer.isNBase(bases[i])) {
				numberOfNs++;
				bases[i] = substBase;
			}
		}
		return numberOfNs;
	}

	public abstract void printConfig();
	public abstract void determineQsOffset(FileSystem fs, Path file) throws CorrectionAlgorithmException; 
	public abstract void determineQsCutoff(int[] qsHistogram) throws CorrectionAlgorithmException;
	public abstract void determineKmerCutoff(int[] kmerHistogram) throws CorrectionAlgorithmException;
	public abstract Sequence correctRead(Sequence sequence);
}
