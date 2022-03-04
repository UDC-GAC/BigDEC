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
package es.udc.gac.bigdec.ec.musket;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.udc.gac.bigdec.ec.Correction;
import es.udc.gac.bigdec.ec.CorrectionAlgorithm;
import es.udc.gac.bigdec.ec.CorrectionAlgorithmException;
import es.udc.gac.bigdec.ec.ErrorCorrection;
import es.udc.gac.bigdec.ec.SolidRegion;
import es.udc.gac.bigdec.ec.SolidRegionComp;
import es.udc.gac.bigdec.kmer.Kmer;
import es.udc.gac.bigdec.kmer.KmerGenerator;
import es.udc.gac.bigdec.sequence.Sequence;
import es.udc.gac.bigdec.util.Configuration;
import es.udc.gac.bigdec.util.IOUtils;

public class MUSKET extends CorrectionAlgorithm {

	private static final long serialVersionUID = -2414857720578019984L;
	private static final Logger logger = LoggerFactory.getLogger(MUSKET.class);

	private int MAX_ITERS;
	private int MAX_ERRORS;
	private int MAX_TRIM;

	public MUSKET(Configuration config, byte kmerLength, int numberOfAlgorithms) {
		super(kmerLength, numberOfAlgorithms);
		MAX_ITERS = config.MUSKET_MAX_ITERS;
		MAX_ERRORS = config.MUSKET_MAX_ERRORS;
		MAX_TRIM = config.MUSKET_MAX_TRIM;
	}

	@Override
	public String toString() {
		return "MUSKET";
	}

	public void printConfig() {
		IOUtils.info("############# "+this.toString()+" #############");
		IOUtils.info("KMER_LENGTH = " +getKmerLength());
		IOUtils.info("KMER_THRESHOLD = " +getKmerThreshold());
		IOUtils.info("MAX_ITERS = " +MAX_ITERS);
		IOUtils.info("MAX_ERRORS = " +MAX_ERRORS);
		IOUtils.info("MAX_TRIM = " +MAX_TRIM);
		IOUtils.info("##################################");
	}

	public void determineQsOffset(FileSystem fs, Path file) throws CorrectionAlgorithmException {}
	public void determineQsCutoff(int[] qsHistogram) throws CorrectionAlgorithmException {}

	public void determineKmerCutoff(int[] kmerHistogram) throws CorrectionAlgorithmException {
		int valleyIndex, peakIndex, i;
		long minMulti, maxMulti;
		short lowerBoundMulti = 100;
		int length = 2;

		for (i = 3; i < ErrorCorrection.KMER_HISTOGRAM_SIZE; i++) {
			if (kmerHistogram[i] != 0)
				length = i;
		}

		logger.debug("length = {}", length);

		/* Reaching the plateau */
		valleyIndex = 2;
		for (i = valleyIndex + 1; i < length; i++) {
			if (kmerHistogram[i] > kmerHistogram[valleyIndex]) {
				break;
			}
			valleyIndex++;
		}
		valleyIndex++;

		/* Find the peak */
		peakIndex = valleyIndex;
		maxMulti = kmerHistogram[peakIndex];
		for (i = peakIndex + 1; i < length; i++) {
			if (kmerHistogram[i] > maxMulti) {
				maxMulti = kmerHistogram[i];
				peakIndex = i;
			}
		}

		/* Find the smallest frequency around the valley */
		minMulti = kmerHistogram[valleyIndex];
		for (i = valleyIndex + 1; i < peakIndex; i++) {
			if (kmerHistogram[i] < lowerBoundMulti) {
				continue;
			}
			if (kmerHistogram[i] < minMulti) {
				minMulti = kmerHistogram[i];
				valleyIndex = i;
			}
		}

		setKmerThreshold(valleyIndex);

		logger.debug("kmer threshold from histogram = {}", getKmerThreshold());
	}

	public Sequence correctRead(Sequence sequence) {
		List<Correction> corrections = new ArrayList<Correction>();
		List<Correction> correctionsAux = new ArrayList<Correction>();
		List<MutablePair<Byte,Short>> bases = new ArrayList<MutablePair<Byte,Short>>();
		List<MutablePair<Byte,Short>> lbases = new ArrayList<MutablePair<Byte,Short>>();
		PriorityQueue<SolidRegion> regionsPingComp = new PriorityQueue<SolidRegion>(new SolidRegionComp.CorrectRegionPingComp());
		PriorityQueue<SolidRegion> regionsPongComp = new PriorityQueue<SolidRegion>(new SolidRegionComp.CorrectRegionPongComp());
		Correction corr = new Correction();
		Kmer km = KmerGenerator.createKmer();
		Kmer lkm = KmerGenerator.createKmer();
		Kmer rep = KmerGenerator.createKmer();
		Kmer lrep = KmerGenerator.createKmer();
		Kmer kmAux1 = KmerGenerator.createKmer();
		Kmer kmAux2 = KmerGenerator.createKmer();
		PriorityQueue<SolidRegion> regions;
		Correction auxCorr;
		BitSet solids = new BitSet(sequence.getLength());
		int[][] votes = new int[sequence.getLength()][4];
		int i, j, nerr, numCorrections, maxVote, minVote = 3;
		int size = sequence.getLength() - getKmerLength();
		boolean pingComp;

		if (logger.isTraceEnabled())
			logger.trace("Correcting {}", sequence.basesToString());

		/*String seq = "CAGACGGAGGTTGGGGTGGGGGGGGGGGTAGTCTTGGTTGGTGGGCACNGTGTGGGNGNCNGNNNTTTGGGGGTG";
		if (!sequence.basesToString(kSize).equals(seq))
			return sequence;
		 */

		// Stage 1: conservative correcting using two-sided correcting
		for (i = 0; i < MAX_ITERS; i++) {
			if ((numCorrections = CorrectCoreTwoSides(sequence, corr, bases, lbases, km, rep, lkm, lrep, 
					kmAux1, kmAux2, solids, getKmerThreshold(), size)) == 0) {
				// error free
				if (logger.isTraceEnabled()) {
					logger.trace("Error free (1)");
					logger.trace("sequenceMod {}", sequence.basesToString());
				}
				return sequence;
			}

			if (numCorrections < 0) // Negative value means more than 1 error was found
				break;

			// Make changes to the input sequence
			logger.trace("Correction(1): base {}, index {}", corr.getBase(), corr.getIndex());
			sequence.getBases()[corr.getIndex()] = Kmer.DECODE[corr.getBase()];
		}

		// Stage 2: aggressive correcting from one side
		for (nerr = 1; nerr <= MAX_ERRORS; nerr++) {
			pingComp = true;
			for (i = 0; i < MAX_ITERS; i++) {
				regions = pingComp? regionsPingComp : regionsPongComp;
				pingComp = !pingComp;

				if (CorrectCoreOneSide(sequence, corrections, correctionsAux, bases, km, rep, lkm, lrep, nerr, MAX_ERRORS - nerr + 1, 
						regions, solids, getKmerThreshold(), size) == true) {
					// error free
					if (logger.isTraceEnabled()) {
						logger.trace("Error free (2)");
						logger.trace("sequenceMod {}", sequence.basesToString());
					}
					return sequence;
				}

				if (corrections.size() == 0)
					break;

				// Make changes to the input sequence
				for(j = 0; j < corrections.size(); j++) {
					auxCorr = corrections.get(j);
					logger.trace("Correction(2): base {}, index {}", auxCorr.getBase(), auxCorr.getIndex());
					sequence.getBases()[auxCorr.getIndex()] = Kmer.DECODE[auxCorr.getBase()];
				}
			}

			// Stage 3: voting and fix erroneous bases
			maxVote = voteErroneousRead(sequence, km, rep, lkm, lrep, votes, getKmerThreshold(), size);

			if (maxVote == 0) {
				// error free
				if (logger.isTraceEnabled()) {
					logger.trace("Error free (3)");
					logger.trace("sequenceMod {}", sequence.basesToString());
				}
				return sequence;
			} else if (maxVote >= minVote) {
				fixErroneousBase(sequence.getLength(), corrections, votes, maxVote);

				// Make changes to the input sequence
				for(j = 0; j < corrections.size(); j++) {
					auxCorr = corrections.get(j);
					logger.trace("Correction(3): base {}, index {}", auxCorr.getBase(), auxCorr.getIndex());
					sequence.getBases()[auxCorr.getIndex()] = Kmer.DECODE[auxCorr.getBase()];
				}
			}
		}

		if (MAX_TRIM > 0) {
			MutablePair<Short,Short> longestRegion = new MutablePair<Short,Short>((short)0, (short)-1);

			// Attempt to trim the sequence using the largest k-mer size
			if (isTrimmable(sequence, km, rep, longestRegion, getKmerThreshold(), size)) {
				logger.trace("Trimmable");

				int seqLen = longestRegion.getRight() - longestRegion.getLeft();

				if (longestRegion.getLeft() > 0) {
					sequence.setBases(sequence.getBases(), longestRegion.getLeft(), seqLen);

					// Base quality scores
					if (sequence.getQuals() != null)
						sequence.setQuals(sequence.getQuals(), longestRegion.getLeft(), seqLen);
				}
			}
		}

		if (logger.isTraceEnabled())
			logger.trace("sequenceMod {}", sequence.basesToString());

		return sequence;
	}

	/*
	 * Error correcting that only allows 1 error in any k-mer of a read
	 */
	private int CorrectCoreTwoSides(Sequence sequence, Correction correction, 
			List<MutablePair<Byte,Short>> bases, List<MutablePair<Byte,Short>> lbases, 
			Kmer km, Kmer rep, Kmer lkm, Kmer lrep, Kmer kmAux1, Kmer kmAux2, BitSet solids, int watershed, int size) {
		byte[] seq = sequence.getBases();
		int seqLen = sequence.getLength();
		byte kSize = getKmerLength();
		int numBases, lnumBases, ipos, i, j, numCorrections;
		byte base, lbase;

		solids.clear();

		/* Iterate each k-mer to check its solidity */
		km.set(seq, 0, kSize);

		for (ipos = 0; ipos <= size; ipos++) {
			if (ipos > 0)
				km.forwardBase(seq[ipos + kSize - 1], kSize);

			if (getKmerMultiplicity(km, rep) >= watershed) {
				for (i = ipos; i < ipos + kSize; i++)
					solids.set(i);
			}
		}

		/* if the read is error-free */
		if (solids.cardinality() == seqLen)
			return 0;

		/* Fix the unique errors relying on k-mer neighboring information */
		km.set(seq, 0, kSize);
		lkm.set(km);

		if (logger.isTraceEnabled()) {
			logger.trace("CorrectCoreTwoSides");
			logger.trace("km  {}", km.basesToString(kSize));
			logger.trace("lkm {}", lkm.basesToString(kSize));
		}

		/* For bases from index 0 to index size (seqLen - kSize) */
		for (ipos = 0; ipos <= size; ipos++) {
			if (ipos > 0)
				km.forwardBase(seq[ipos + kSize - 1], kSize);

			if (ipos >= kSize)
				lkm.forwardBase(seq[ipos], kSize);

			if (solids.get(ipos))
				continue;

			/* Try to find all mutations for the rightmost k-mer */
			numBases = selectAllMutations(km, kmAux1, kmAux2, 0, bases, watershed);

			/* Try to find all mutations for the leftmost k-mer */
			lnumBases = selectAllMutations(lkm, kmAux1, kmAux2, ipos >= kSize? kSize - 1 : ipos, lbases, watershed);

			numCorrections = 0;

			/* If the two bases are the same, this base is modified */
			for (i = 0; i < numBases && numCorrections <= 1; i++) {
				base = bases.get(i).left;
				for (j = 0; j < lnumBases; j++) {
					lbase = lbases.get(j).left;
					if (base == lbase) {
						numCorrections++;
						correction.setBase(base);
						correction.setIndex((short)ipos);
					}
				}
			}

			/* Check if only one correction is found */
			if (numCorrections == 1)
				return 1;
		}

		/* For the remaining k-1 bases */
		for (; ipos < seqLen; ipos++) {
			lkm.forwardBase(seq[ipos], kSize);

			if (solids.get(ipos))
				continue;

			/* Try to fix using the current k-mer */
			numBases = selectAllMutations(km, kmAux1, kmAux2, ipos - size, bases, watershed);

			/* Try to fix using the left k-mer */
			lnumBases = selectAllMutations(lkm, kmAux1, kmAux2, kSize - 1, lbases, watershed);

			numCorrections = 0;

			/* If the two bases are the same, this base is modified */
			for (i = 0; i < numBases && numCorrections <= 1; i++) {
				base = bases.get(i).left;
				for (j = 0; j < lnumBases; j++) {
					lbase = lbases.get(j).left;
					if (base == lbase) {
						numCorrections++;
						correction.setBase(base);
						correction.setIndex((short)ipos);
					}
				}
			}

			/* Check if only one correction is found */
			if (numCorrections == 1)
				return 1;
		}

		/* Not error-free */
		return -1;
	}

	/*
	 * Core function of error correction without gaps
	 */
	private boolean CorrectCoreOneSide(Sequence sequence, List<Correction> corrections, List<Correction> correctionsAux, 
			List<MutablePair<Byte,Short>> bases, Kmer km, Kmer rep, Kmer kmAux1, Kmer kmAux2, int maxErrorPerKmer, 
			int stride, PriorityQueue<SolidRegion> regions, BitSet solids, int watershed, int size) {
		MutablePair<Byte,Short> best = new MutablePair<Byte,Short>();
		byte[] seq = sequence.getBases();
		int seqLen = sequence.getLength();
		byte kSize = getKmerLength();
		int numBases, ipos, i, targetPos, lastPosition, numCorrectionsPerKmer;
		int leftKmer = -1, rightKmer = -1;
		boolean solidRegion = false, done;
		SolidRegion region;

		solids.clear();
		corrections.clear();
		regions.clear();

		km.set(seq, 0, kSize);

		if (logger.isTraceEnabled())
			logger.trace("CorrectCoreOneSide: km {}", km.basesToString(kSize));

		for (ipos = 0; ipos <= size; ipos++) {
			if (ipos > 0)
				km.forwardBase(seq[ipos + kSize - 1], kSize);

			if (getKmerMultiplicity(km, rep) >= watershed) {
				// Found a healthy k-mer!
				if (!solidRegion) {
					solidRegion = true;
					leftKmer = rightKmer = ipos;
				} else {
					rightKmer++;
				}

				for (i = ipos; i < ipos + kSize; i++)
					solids.set(i);
			} else {
				// Save the trusted region
				if (leftKmer >= 0) {
					regions.add(new SolidRegion(leftKmer, rightKmer));
					leftKmer = rightKmer = -1;
				}
				solidRegion = false;
			}
		}

		if (solidRegion && leftKmer >= 0)
			regions.add(new SolidRegion(leftKmer, rightKmer));

		if (regions.size() == 0)
			/* This read is error-free */
			return true;

		if (logger.isTraceEnabled()) {
			logger.trace("solids {}", solids.cardinality());
			for(SolidRegion reg: regions)
				logger.trace(reg.getLeftKmer() + " " + reg.getRightKmer() + " " + (reg.getRightKmer() - reg.getLeftKmer() + 1));
		}

		/* Calculate the minimal number of votes per base */
		while (regions.size() > 0) {
			/* Get the region with the highest priority */
			region = regions.poll();
			leftKmer = region.getLeftKmer();
			rightKmer = region.getRightKmer();

			/* Form the starting k-mer */
			km.set(seq, rightKmer, kSize);

			//if (logger.isTraceEnabled())
			//logger.trace("km: {}, leftKmer: {}, rightKmer: {}", km.basesToString(kSize), leftKmer, rightKmer);

			lastPosition = -1;
			numCorrectionsPerKmer = 0;
			correctionsAux.clear();

			for (ipos = rightKmer + 1; ipos <= size; ipos++) {
				targetPos = ipos + kSize - 1;

				/* Check the solids of the k-mer */
				if (solids.get(targetPos)) {
					/* If it reaches another trusted region */
					break;
				}

				km.forwardBase(seq[targetPos], kSize);

				if (getKmerMultiplicity(km, rep) < watershed) {
					/* Select all possible mutations */
					numBases = selectAllMutations(km, kmAux1, kmAux2, kSize - 1, bases, watershed);

					/* Start correcting */
					done = false;

					if (numBases == 1) {
						byte base = bases.get(0).left;
						correctionsAux.add(new Correction((short)targetPos, base));
						/* Set the last base */
						km.setBase(base, kSize - 1);
						done = true;
						if (logger.isTraceEnabled()) {
							logger.trace("km: {}", km.basesToString(kSize));
							logger.trace("base: {}, tpos: {}, multi: {}", base, targetPos, bases.get(0).right);
						}
					} else {
						/* Select the best substitution */
						best.setLeft((byte)0);
						best.setRight((short)0);

						for (i = 0; i < numBases; i++) {
							byte base = bases.get(i).left;
							kmAux1.set(km);
							kmAux1.setBase(base, kSize - 1);

							if (successor(seq, ipos + 1, seqLen - ipos - 1, kmAux1, kmAux2, stride, watershed)) {
								/* Check the multiplicity */
								if (best.getRight() < bases.get(i).right) {
									best = bases.get(i);
								}
							}
						}

						/* If finding a best one */
						if (best.getRight() > 0) {
							correctionsAux.add(new Correction((short)targetPos, best.getLeft()));
							km.setBase(best.getLeft(), kSize - 1);
							done = true;
							if (logger.isTraceEnabled()) {
								logger.trace("km: {}", km.basesToString(kSize));
								logger.trace("tpos: {}, numBases: {}, best.left: {}, best.right: {}", targetPos, numBases, best.getLeft(), best.getRight());
							}
						}
					}

					/* If finding one correction */
					if (done) {
						/* Recording the position */
						if (lastPosition < 0)
							lastPosition = targetPos;

						/* Check the number of errors in any k-mer length */
						if (targetPos - lastPosition < kSize) {
							/* Increase the number of corrections */
							numCorrectionsPerKmer++;

							if (numCorrectionsPerKmer > maxErrorPerKmer) {
								for (i = 0; i < numCorrectionsPerKmer; i++) {
									correctionsAux.remove(correctionsAux.size() - 1);
								}
								break;
							}
						} else {
							lastPosition = targetPos;
							numCorrectionsPerKmer = 0;
						}
						continue;
					}
					break; // Check the next region
				} // end if
			} // end for

			/* Save the corrections in this region */
			corrections.addAll(correctionsAux);

			/* Towards the beginning of the sequence from this position */
			lastPosition = (correctionsAux.size() > 0)? correctionsAux.get(0).getIndex() : -1;
			numCorrectionsPerKmer = 0;
			correctionsAux.clear();

			if (leftKmer > 0) {
				km.set(seq, leftKmer, kSize);

				for (ipos = leftKmer - 1; ipos >= 0; ipos--) {
					if (solids.get(ipos))
						/* If it reaches another trusted region */
						break;

					km.backwardBase(seq[ipos], kSize);

					if (getKmerMultiplicity(km, rep) < watershed) {
						/* Select all possible mutations */
						numBases = selectAllMutations(km, kmAux1, kmAux2, 0, bases, watershed);

						/* Start correcting */
						done = false;

						if (numBases == 1) {
							byte base = bases.get(0).left;
							correctionsAux.add(new Correction((short)ipos, base));
							/* Set the last base */
							km.setBase(base, 0);
							done = true;
							if (logger.isTraceEnabled()) {
								logger.trace("km: {}", km.basesToString(kSize));
								logger.trace("base: {}, ipos: {}, multi: {}", base, ipos, bases.get(0).right);
							}
						} else {
							/* Select the best substitution */
							best.setLeft((byte)0);
							best.setRight((short)0);

							for (i = 0; i < numBases; i++) {
								byte base = bases.get(i).left;
								kmAux1.set(km);
								kmAux1.setBase(base, 0);

								if (predecessor(seq, 0, ipos - 1, kmAux1, kmAux2, stride, watershed)) {
									if (best.getRight() < bases.get(i).right) {
										best = bases.get(i);
									}
								}
							}

							/* If finding a best one */
							if (best.getRight() > 0) {
								correctionsAux.add(new Correction((short)ipos, best.getLeft()));
								km.setBase(best.getLeft(), 0);
								done = true;
								if (logger.isTraceEnabled()) {
									logger.trace("km: {}", km.basesToString(kSize));
									logger.trace("ipos: {}, numBases: {}, best.left: {}, best.right: {}", ipos, numBases, best.getLeft(), best.getRight());
								}
							}
						}

						/* If finding one correction */
						if (done) {
							/* Recording the position */
							if (lastPosition < 0)
								lastPosition = ipos;

							/* Check the number of errors in any k-mer length */
							if (lastPosition - ipos < kSize) {
								/* Increase the number of corrections */
								numCorrectionsPerKmer++;

								if (numCorrectionsPerKmer > maxErrorPerKmer) {
									for (i = 0; i < numCorrectionsPerKmer; i++) {
										correctionsAux.remove(correctionsAux.size() - 1);
									}
									break;
								}
							} else {
								lastPosition = ipos;
								numCorrectionsPerKmer = 0;
							}
							continue;
						}
						break; // Check the next region
					} // end if
				} // end for
			} // end if leftKmer > 0

			/* Save the corrections in this region */
			corrections.addAll(correctionsAux);
		} // end while

		/* Not error-free */
		return false;
	}

	private void fixErroneousBase(int seqLen, List<Correction> corrections, int[][] votes, int maxVote) {
		int alternativeBase, pos, base;

		corrections.clear();

		/* Find the qualified base mutations */
		for (pos = 0; pos < seqLen; pos++) {
			alternativeBase = -1;
			for (base = 0; base < 4; base++) {
				if (votes[pos][base] == maxVote) {
					if (alternativeBase == -1) {
						alternativeBase = base;
					} else {
						alternativeBase = -1;
						break;
					}
				}
			}

			if (alternativeBase >= 0)
				corrections.add(new Correction((short)pos, (byte)alternativeBase));
		}
	}

	private int voteErroneousRead(Sequence sequence, Kmer km, Kmer rep, Kmer alternativeKmer, Kmer alternativeSearch, 
			int[][] votes, int watershed, int size) {
		// Cast votes for mutations
		int ipos, off, baseIndex, originalBase, base, vote, maxVote = 0;
		byte[] seq = sequence.getBases();
		int seqLen = sequence.getLength();
		byte kSize = getKmerLength();
		boolean errorFree = true, revcomp;

		km.set(seq, 0, kSize);

		// Initialize the votes matrix
		for (ipos = 0; ipos < seqLen; ipos++) {
			for (base = 0; base < 4; base++) {
				votes[ipos][base] = 0;
			}
		}

		for (ipos = 0; ipos <= size; ipos++) {
			if (ipos > 0)
				km.forwardBase(seq[ipos + kSize - 1], kSize);

			if (getKmerMultiplicity(km, alternativeSearch) >= watershed)
				continue;

			errorFree = false;

			revcomp = true;
			rep.twin(km, kSize);
			if(MUSKET.compare(km, rep)) {
				rep.set(km); // rep = km
				revcomp = false;
			}

			// For each offset
			for (off = 0; off < kSize; off++) {
				/* For each possible mutation */
				baseIndex = revcomp ? kSize - off - 1 : off;
				originalBase = rep.getBase(baseIndex);

				/* Get all possible alternatives */
				for (base = (originalBase + 1) & 3; base != originalBase; base = (base + 1) & 3) {
					alternativeKmer.set(rep);
					alternativeKmer.setBase((byte)base, baseIndex);

					if (getKmerMultiplicity(alternativeKmer, alternativeSearch) >= watershed)
						votes[ipos + off][revcomp? base ^ 3 : base]++;
				}
			}
		}

		/* Error-free read */
		if (errorFree)
			return 0;

		/* Select the maximum vote */
		for (ipos = 0; ipos < seqLen; ipos++) {
			for (base = 0; base < 4; base++) {
				vote = votes[ipos][base];
				if (vote > maxVote)
					maxVote = vote;
			}
		}

		logger.trace("maxVote: {}", maxVote);
		return maxVote;
	}

	private boolean isTrimmable(Sequence sequence, Kmer km, Kmer rep, MutablePair<Short,Short> region, int watershed, int size) {
		byte[] seq = sequence.getBases();
		int seqLen = sequence.getLength();
		byte kSize = getKmerLength();
		int ipos, leftKmer = -1, rightKmer = -1;
		boolean trustedRegion = true;

		km.set(seq, 0, kSize);

		/* Check the trustiness of each k-mer */
		for (ipos = 0; ipos <= size; ipos++) {
			if (ipos > 0)
				km.forwardBase(seq[ipos + kSize - 1], kSize);

			if (getKmerMultiplicity(km, rep) >= watershed) {
				// Found a healthy k-mer!
				if (trustedRegion) {
					trustedRegion = false;
					leftKmer = rightKmer = ipos;
				} else {
					rightKmer++;
				}
			} else {
				// Save the trusted region
				if (leftKmer >= 0) {
					if (rightKmer - leftKmer > region.getRight() - region.getLeft()) {
						region.setLeft((short)leftKmer);
						region.setRight((short)rightKmer);
					}
					leftKmer = rightKmer = -1;
				}
				trustedRegion = true;
			}
		}

		if (trustedRegion == false && leftKmer >= 0) {
			if (rightKmer - leftKmer > region.getRight() - region.getLeft()) {
				region.setLeft((short)leftKmer);
				region.setRight((short)rightKmer);
			}
		}

		if (region.getRight() < region.getLeft())
			return false;

		/* Check the longest trusted region */
		region.setRight((short)(region.getRight() + kSize));

		if (seqLen - (region.getRight() - region.getLeft()) > MAX_TRIM)
			return false;

		return true;
	}

	private int selectAllMutations(Kmer km, Kmer alternativeKmer, Kmer alternativeSearch, int index, 
			List<MutablePair<Byte,Short>> bases, int watershed) {
		int base;
		int multi;
		byte kSize = getKmerLength();
		boolean revcomp;

		bases.clear();

		alternativeKmer.twin(km, kSize);
		revcomp = true;

		if(MUSKET.compare(km, alternativeKmer)) {
			alternativeKmer.set(km);
			revcomp = false;
		}

		/* Get the original base at index */
		int baseIndex = revcomp ? kSize - 1 - index : index;
		int originalBase = alternativeKmer.getBase(baseIndex);

		if (logger.isTraceEnabled()) {
			logger.trace("km: {}", alternativeKmer.basesToString(kSize));
			logger.trace("revcomp {}, baseIndex {}", revcomp, baseIndex);
		}

		/* Get all possible alternatives */
		for (base = (originalBase + 1) & 3; base != originalBase; base = (base + 1) & 3) {
			alternativeKmer.setBase((byte)base, baseIndex);

			/* Get k-mer multiplicity */
			multi = getKmerMultiplicity(alternativeKmer, alternativeSearch);

			if (multi >= watershed)
				bases.add(new MutablePair<Byte,Short>(revcomp? (byte)(base ^ 3) : (byte)base, (short)multi));
		}

		return bases.size();
	}

	/*
	 * Check the correctness of its successing k-mer
	 */
	private boolean successor(byte[] seq, int offset, int seqLen, Kmer km, Kmer rep, int dist, int watershed) {
		byte kSize = getKmerLength();

		if (seqLen < kSize || dist <=0)
			return true;

		int endPos = Math.min(seqLen - kSize, dist - 1);
		int ipos;

		for (ipos = 0; ipos <= endPos; ipos++) {
			km.forwardBase(seq[offset + ipos + kSize - 1], kSize);

			if (getKmerMultiplicity(km, rep) < watershed)
				return false;
		}

		return true;
	}

	/*
	 * Check the correctness of its precessing k-mer
	 */
	private boolean predecessor(byte[] seq, int offset, int seqLen, Kmer km, Kmer rep, int dist, int watershed) {
		if (seqLen <= 0 || dist <=0)
			return true;

		byte kSize = getKmerLength();
		int startPos = Math.max(0, seqLen - dist);
		int ipos;

		for (ipos = seqLen - 1; ipos >= startPos; ipos--) {
			km.backwardBase(seq[offset + ipos], kSize);

			if (getKmerMultiplicity(km, rep) < watershed)
				return false;
		}

		return true;
	}

	/*
	 * This method compares k-mers in a byte-by-byte way, as Musket
	 * does to decide which k-mer is used when selecting all possible
	 * mutations.
	 * 
	 * It returns true when the first kmer is lower than the second one
	 */
	private static boolean compare(Kmer kmer1, Kmer kmer2) {
		byte b1 = (byte) (kmer1.getBases() >>> 56);
		byte b2 = (byte) (kmer2.getBases() >>> 56);

		b1 = (byte) ((b1 & 3) << 6 |
				(((b1 >>> 2) & 3) << 4) |
				(((b1 >>> 4) & 3) << 2) |
				((b1 >>> 6) & 3));
		b2 = (byte) ((b2 & 3) << 6 |
				(((b2 >>> 2) & 3) << 4) |
				(((b2 >>> 4) & 3) << 2) |
				((b2 >>> 6) & 3));

		if (b1 != b2)
			return ((b1 & 0xFF) - (b2 & 0xFF)) < 0;

		for (int i = 6; i >= 0; i--) {
			b1 = (byte) (kmer1.getBases() >>> (8*i));
			b2 = (byte) (kmer2.getBases() >>> (8*i));
			b1 = (byte) ((b1 & 3) << 6 |
					(((b1 >>> 2) & 3) << 4) |
					(((b1 >>> 4) & 3) << 2) |
					((b1 >>> 6) & 3));
			b2 = (byte) ((b2 & 3) << 6 |
					(((b2 >>> 2) & 3) << 4) |
					(((b2 >>> 4) & 3) << 2) |
					((b2 >>> 6) & 3));
			if (b1 != b2)
				return ((b1 & 0xFF) - (b2 & 0xFF)) < 0;
		}

		return false;
	}
}
