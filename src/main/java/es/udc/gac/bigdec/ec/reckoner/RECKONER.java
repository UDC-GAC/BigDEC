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
package es.udc.gac.bigdec.ec.reckoner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.udc.gac.bigdec.ec.Correction;
import es.udc.gac.bigdec.ec.CorrectionAlgorithm;
import es.udc.gac.bigdec.ec.CorrectionAlgorithmException;
import es.udc.gac.bigdec.ec.ErrorCorrection;
import es.udc.gac.bigdec.ec.SolidRegion;
import es.udc.gac.bigdec.kmer.Kmer;
import es.udc.gac.bigdec.kmer.KmerGenerator;
import es.udc.gac.bigdec.sequence.Sequence;
import es.udc.gac.bigdec.util.Configuration;
import es.udc.gac.bigdec.util.IOUtils;

public class RECKONER extends CorrectionAlgorithm {

	private static final long serialVersionUID = -8812450735689566722L;
	private static final Logger logger = LoggerFactory.getLogger(RECKONER.class);

	private int MAX_KMER_THRESHOLD;
	private int MAX_EXTEND;
	private byte QS_OFFSET;
	private byte QS_CUTOFF;
	private double MAX_N_RATIO;
	private double MAX_ERROR_RATIO;
	private byte SUBST_BASE;
	private byte PHRED33;
	private byte PHRED64;
	private byte MIN_33_SCORE;
	private byte MAX_64_SCORE;
	private int MIN_SOLID_LENGTH;
	private int MIN_NON_SOLID_LENGTH;
	private int MAX_DETECT_SCORE_DIFF;
	private int FP_SUSPECT_LENGTH;
	private int SOLID_REGION_ADJUST_RANGE;
	private int MAX_EXTEND_CORRECTION_PATHS;
	private int CHECK_MAX_CHANGES;
	private int MAX_FIRST_KMER_POSSIBILITIES;
	private int MAX_FIRST_KMER_CORRECTION_PATHS;
	private int MAX_LOW_QS_BASES;
	private int MAX_LOW_QS_INDEXES_COMB;
	private int MAX_CHECK_FIRST_KMER_NESTING;
	private double COVERING_KMERS_WEIGHT;
	private double EXTENSION_KMERS_WEIGHT;
	private double MIN_BEST_KMER_QUALITY;
	private double MAX_CHANGES_IN_REGION_RATIO;
	private boolean LIMIT_MODIFICATIONS;

	public RECKONER(Configuration config, byte kmerLength, int numberOfAlgorithms) {
		super(kmerLength, numberOfAlgorithms);
		MAX_KMER_THRESHOLD = config.RECKONER_MAX_KMER_THRESHOLD;
		MAX_EXTEND = config.RECKONER_MAX_EXTEND;
		QS_CUTOFF = config.RECKONER_QS_CUTOFF;
		MAX_N_RATIO = config.RECKONER_MAX_N_RATIO;
		MAX_ERROR_RATIO = config.RECKONER_MAX_ERROR_RATIO;
		SUBST_BASE = config.RECKONER_SUBST_BASE.getBytes(StandardCharsets.US_ASCII)[0];
		PHRED33 = config.RECKONER_PHRED33;
		PHRED64 = config.RECKONER_PHRED64;
		MIN_33_SCORE = config.RECKONER_MIN_33_SCORE;
		MAX_64_SCORE = config.RECKONER_MAX_64_SCORE;
		MIN_SOLID_LENGTH = config.RECKONER_MIN_SOLID_LENGTH;
		MIN_NON_SOLID_LENGTH = config.RECKONER_MIN_NON_SOLID_LENGTH;
		MAX_DETECT_SCORE_DIFF = config.RECKONER_MAX_DETECT_SCORE_DIFF;
		FP_SUSPECT_LENGTH = config.RECKONER_FP_SUSPECT_LENGTH;
		SOLID_REGION_ADJUST_RANGE = config.RECKONER_SOLID_REGION_ADJUST_RANGE;
		MAX_EXTEND_CORRECTION_PATHS = config.RECKONER_MAX_EXTEND_CORRECTION_PATHS;
		CHECK_MAX_CHANGES = config.RECKONER_CHECK_MAX_CHANGES;
		MAX_FIRST_KMER_POSSIBILITIES = config.RECKONER_MAX_FIRST_KMER_POSSIBILITIES;
		MAX_FIRST_KMER_CORRECTION_PATHS = config.RECKONER_MAX_FIRST_KMER_CORRECTION_PATHS;
		MAX_LOW_QS_BASES = config.RECKONER_MAX_LOW_QS_BASES;
		MAX_LOW_QS_INDEXES_COMB = config.RECKONER_MAX_LOW_QS_INDEXES_COMB;
		MAX_CHECK_FIRST_KMER_NESTING = Math.max(MAX_LOW_QS_BASES, MAX_LOW_QS_INDEXES_COMB);
		COVERING_KMERS_WEIGHT = config.RECKONER_COVERING_KMERS_WEIGHT;
		EXTENSION_KMERS_WEIGHT = config.RECKONER_EXTENSION_KMERS_WEIGHT;
		MIN_BEST_KMER_QUALITY = config.RECKONER_MIN_BEST_KMER_QUALITY;
		MAX_CHANGES_IN_REGION_RATIO = config.RECKONER_MAX_CHANGES_IN_REGION_RATIO;
		LIMIT_MODIFICATIONS = config.RECKONER_LIMIT_MODIFICATIONS;
	}

	@Override
	public String toString() {
		return "RECKONER";
	}

	public void printConfig() {
		IOUtils.info("############# "+this.toString()+" #############");
		IOUtils.info("KMER_LENGTH = " +getKmerLength());
		IOUtils.info("KMER_THRESHOLD = " +getKmerThreshold());
		IOUtils.info("QS_OFFSET = " +QS_OFFSET);
		IOUtils.info("QS_CUTOFF = " +QS_CUTOFF);
		IOUtils.info("MAX_KMER_THRESHOLD = " +MAX_KMER_THRESHOLD);
		IOUtils.info("MAX_EXTEND = " +MAX_EXTEND);
		IOUtils.info("MAX_N_RATIO = " +MAX_N_RATIO);
		IOUtils.info("MAX_ERROR_RATIO = " +MAX_ERROR_RATIO);
		IOUtils.info("SUBST_BASE = " +new String(new byte[] {SUBST_BASE}, StandardCharsets.US_ASCII));
		IOUtils.info("PHRED33 = " +PHRED33);
		IOUtils.info("PHRED64 = " +PHRED64);
		IOUtils.info("MIN_33_SCORE = " +MIN_33_SCORE);
		IOUtils.info("MAX_64_SCORE = " +MAX_64_SCORE);
		IOUtils.info("MIN_SOLID_LENGTH = " +MIN_SOLID_LENGTH);
		IOUtils.info("MIN_NON_SOLID_LENGTH = " +MIN_NON_SOLID_LENGTH);
		IOUtils.info("MAX_DETECT_SCORE_DIFF = " +MAX_DETECT_SCORE_DIFF);
		IOUtils.info("FP_SUSPECT_LENGTH = " +FP_SUSPECT_LENGTH);
		IOUtils.info("SOLID_REGION_ADJUST_RANGE = " +SOLID_REGION_ADJUST_RANGE);
		IOUtils.info("MAX_EXTEND_CORRECTION_PATHS = " +MAX_EXTEND_CORRECTION_PATHS);
		IOUtils.info("CHECK_MAX_CHANGES = " +CHECK_MAX_CHANGES);	
		IOUtils.info("MAX_FIRST_KMER_POSSIBILITIES = " +MAX_FIRST_KMER_POSSIBILITIES);
		IOUtils.info("MAX_FIRST_KMER_CORRECTION_PATHS = " +MAX_FIRST_KMER_CORRECTION_PATHS);
		IOUtils.info("MAX_LOW_QS_BASES = " +MAX_LOW_QS_BASES);
		IOUtils.info("MAX_LOW_QS_INDEXES_COMB = " +MAX_LOW_QS_INDEXES_COMB);
		IOUtils.info("MAX_CHECK_FIRST_KMER_NESTING = " +MAX_CHECK_FIRST_KMER_NESTING);
		IOUtils.info("COVERING_KMERS_WEIGHT = " +COVERING_KMERS_WEIGHT);
		IOUtils.info("EXTENSION_KMERS_WEIGHT = " +EXTENSION_KMERS_WEIGHT);		
		IOUtils.info("MIN_BEST_KMER_QUALITY = " +MIN_BEST_KMER_QUALITY);
		IOUtils.info("MAX_CHANGES_IN_REGION_RATIO = " +MAX_CHANGES_IN_REGION_RATIO);
		IOUtils.info("LIMIT_MODIFICATIONS = " +LIMIT_MODIFICATIONS);
		IOUtils.info("##################################");
	}

	public void determineQsOffset(FileSystem fs, Path file) throws CorrectionAlgorithmException {
		BufferedReader br = null;
		FSDataInputStream dis = null;
		String line;
		boolean qsOffsetFound = false;
		QS_OFFSET = PHRED33;

		try {
			IOUtils.info(this.toString()+": checking reads");

			dis = fs.open(file);
			br = new BufferedReader(new InputStreamReader(dis));

			while (!qsOffsetFound && br.readLine() != null) {
				// Bases
				br.readLine();
				// Connector
				br.readLine();
				// Quality scores
				line = br.readLine();

				for (int i = 0; i < line.length(); i++) {
					if (line.charAt(i) > MAX_64_SCORE - MAX_DETECT_SCORE_DIFF) {
						QS_OFFSET = PHRED64;
						qsOffsetFound = true;
						break;
					}
					if (line.charAt(i) < MIN_33_SCORE + MAX_DETECT_SCORE_DIFF) {
						QS_OFFSET = PHRED33;
						qsOffsetFound = true;
						break;
					}
				}
			}

			if (br != null)
				br.close();
			if (dis != null)
				dis.close();
		} catch (IOException e) {
			throw new CorrectionAlgorithmException(e.getMessage());
		}

		IOUtils.info("quality score offset = " +QS_OFFSET);
	}

	public void determineQsCutoff(int[] qsHistogram) throws CorrectionAlgorithmException {
		/*
		 * The quality score threshold is set through configuration
		 */
		IOUtils.info("quality score threshold = " +QS_CUTOFF);
	}

	public void determineKmerCutoff(int[] kmerHistogram) throws CorrectionAlgorithmException {
		long firstMin = kmerHistogram[2];
		int cutoff = 2;

		for (int i = 3; i < ErrorCorrection.KMER_HISTOGRAM_SIZE; i++) {
			if (firstMin >= kmerHistogram[i]) {
				firstMin = kmerHistogram[i];
			} else {
				cutoff = i - 1;
				break;
			}
		}

		if (cutoff <= MAX_KMER_THRESHOLD) {
			setKmerThreshold(cutoff);
		} else {
			setKmerThreshold(MAX_KMER_THRESHOLD);
			IOUtils.warn("The automatically determined k-mer threshold ("+cutoff+") is larger than the maximum value ("+MAX_KMER_THRESHOLD+"). The k-mer threshold is set to "+MAX_KMER_THRESHOLD);
		}

		logger.debug("kmer threshold from histogram = {}", getKmerThreshold());
	}

	public Sequence correctRead(Sequence sequence) {
		int i, j, multi, numKmers, it_sr, it_base, it_region, it_adjust, first, second;
		int numOfNs, numLowQualityBase, indexPrevLowQualityBase, prevLowQualityIndex;
		boolean tooManyNs = false, tooManyErrors = false, isSolidKmerPrev = false;
		int seqLength = sequence.getLength();
		byte[] bases = sequence.getBases();
		byte[] quals = sequence.getQuals();
		byte kmerLength = getKmerLength();
		int size = seqLength - kmerLength;
		byte[] basesSafe = new byte[seqLength];
		List<SolidRegion> solidRegions = new ArrayList<SolidRegion>();
		List<SolidRegion> solidRegionsTemp = new ArrayList<SolidRegion>();
		SolidRegion newSolidRegion = new SolidRegion();
		Kmer kmer = KmerGenerator.createKmer();
		Kmer kmerRC = KmerGenerator.createKmer();
		Kmer kmerAux = KmerGenerator.createKmer();
		List<CandidatePath> candidatePaths = new ArrayList<CandidatePath>();
		List<CandidatePath> candidatePathsTemp = new ArrayList<CandidatePath>();
		List<MutablePair<Byte,Integer>> modificationsSequence = new ArrayList<MutablePair<Byte,Integer>>(seqLength);
		int sumErrors = 0;
		int numCorrectedErrorsStep1_1 = 0;
		int numCorrectedErrorsStep1_2 = 0;
		int numCorrectedErrorsStep1_3 = 0;
		int numCorrectedErrorsStep2 = 0;

		System.arraycopy(bases, 0, basesSafe, 0, basesSafe.length);

		if (logger.isTraceEnabled())
			logger.trace("Correcting {}", sequence.basesToString());

		/*String seq = "GACGGTGTGAGGCCGGTAGCGGCCCCCGGCGCGCCGGGCCCGGGACTTCCCGGAGTCGGGTTGCTTGGGAAAGCC";
		if (!sequence.basesToString().equals(seq))
			return sequence;
		 */

		// Replace N bases
		numOfNs = CorrectionAlgorithm.replaceNs(sequence, SUBST_BASE);
		tooManyNs = numOfNs >= (seqLength * MAX_N_RATIO);

		logger.trace("numberOfNs {}, tooManyNs {}", numOfNs, tooManyNs);

		if (tooManyNs) {
			// Revert changes
			sequence.setBases(basesSafe);
			// Skip this read
			return sequence;
		}

		if (numOfNs > 0)
			System.arraycopy(bases, 0, basesSafe, 0, basesSafe.length);

		//--------------------------------------------------
		// STEP 0-0: find solid k-mers in this read
		//--------------------------------------------------
		// Generate k-mers
		numKmers = size + 1;
		kmer.set(bases, 0, kmerLength);
		j = kmerLength;

		for (i = 0; i < numKmers; i++) {
			if (i > 0) {
				kmer.forwardBase(bases[j], kmerLength);
				j++;
			}

			//logger.trace("km {} {}", i, kmer.basesToString());
			multi = getKmerMultiplicity(kmer, kmerRC);

			if (multi != 0) {
				// k-mer is solid
				logger.trace("multi {}", multi);
				// start point of a solid region
				if (isSolidKmerPrev == false) {
					newSolidRegion.first(i);
					isSolidKmerPrev = true;
				}
			} else {
				// end point of a solid region
				if (isSolidKmerPrev == true) {
					newSolidRegion.second(i - 1);

					if (newSolidRegion.second() < newSolidRegion.first()) 
						logger.error("The second index is smaller than the first");

					solidRegions.add(new SolidRegion(newSolidRegion));
					isSolidKmerPrev = false;
				}
			}
		}

		// last solid region
		if (isSolidKmerPrev == true) {
			newSolidRegion.second(numKmers - 1);
			solidRegions.add(new SolidRegion(newSolidRegion));
		}

		//--------------------------------------------------
		// STEP 0-1: adjust solid regions using quality scores
		//--------------------------------------------------
		// solid_regions_org: indexes that are not modified
		// when the indices are reached, all kinds of modifications
		// (A/C/G/T) should be made and checked at least one solid region
		if (solidRegions.size() > 0) {
			// exceptional case: only one solid island that covers the entire read
			if ((solidRegions.size() != 1) || (solidRegions.get(0).first() != 0) || (solidRegions.get(0).second() != size)) {
				if (solidRegions.size() > 1) {
					// at least two solid k-mer islands
					// check the distance between every two solid k-mer islands
					boolean flagShortDistance = false;

					for (i = 0; i < solidRegions.size() - 1; i++) {
						if ((solidRegions.get(i + 1).first() - solidRegions.get(i).second()) < kmerLength) {
							flagShortDistance = true;
						}
					}

					logger.trace("two solid regions, flagShortDistance {}", flagShortDistance);

					if (flagShortDistance == true) {
						// each solid island
						for (it_sr = 0; it_sr < solidRegions.size(); it_sr++) {
							// each base in the solid island (0-base)
							numLowQualityBase = 0;
							// set an initial value to avoid a compilation warning
							indexPrevLowQualityBase = 0;

							for (it_base = solidRegions.get(it_sr).first(); it_base < solidRegions.get(it_sr).second() + kmerLength; it_base++) {
								// a current base has a low quality score
								if ((quals[it_base] - QS_OFFSET) < QS_CUTOFF) {							
									numLowQualityBase++;

									if (numLowQualityBase == 1) {
										// first low quality base
										// the low quality base is not in the first k-mer of the solid island
										first = solidRegions.get(it_sr).first();
										if (it_base >= (first + kmerLength)) {
											// add the left most high quality region to a temporary vector
											solidRegionsTemp.add(new SolidRegion(first, it_base - kmerLength));
										}
									} else {
										// not first low quality base
										if ((it_base - indexPrevLowQualityBase) > kmerLength) {
											solidRegionsTemp.add(new SolidRegion(indexPrevLowQualityBase + 1, it_base - kmerLength));
										}
									}
									indexPrevLowQualityBase = it_base;
								}
							}

							logger.trace("indexPrevLowQualityBase {}, numLowQualityBase {}", indexPrevLowQualityBase, numLowQualityBase);

							// process the bases to the right of the rightmost low quality base
							if (numLowQualityBase > 0) {
								second = solidRegions.get(it_sr).second();
								if (second >= (indexPrevLowQualityBase + kmerLength)) {
									solidRegionsTemp.add(new SolidRegion(indexPrevLowQualityBase + kmerLength, second));
								}
							} else {
								// no low quality base
								// add the current solid island
								solidRegionsTemp.add(solidRegions.get(it_sr));
							}
						}

						solidRegions.clear();
						solidRegions.addAll(solidRegionsTemp);
					}
				} else if (solidRegions.size() == 1) {
					// only one solid k-mer island
					numLowQualityBase = 0;
					prevLowQualityIndex = 0;

					logger.trace("one solid region");

					// each base in the solid island (0-base)
					first = solidRegions.get(0).first();

					for (it_base = first; it_base < solidRegions.get(0).second() + kmerLength; it_base++) {
						// a current base has a low quality score
						if ((quals[it_base] - QS_OFFSET) < QS_CUTOFF) {
							numLowQualityBase++;

							if (numLowQualityBase == 1) {
								// first low quality base

								if ((it_base - first) >= (kmerLength + MIN_SOLID_LENGTH - 1)) {
									solidRegionsTemp.add(new SolidRegion(first, it_base - kmerLength));
								}

								prevLowQualityIndex = it_base;
							} else {
								// not first low quality base
								if ((it_base - prevLowQualityIndex) >= (kmerLength + MIN_SOLID_LENGTH)) {
									solidRegionsTemp.add(new SolidRegion(prevLowQualityIndex + 1, it_base - kmerLength));
								}

								prevLowQualityIndex = it_base;
							}
						}
					}

					logger.trace("prevLowQualityIndex {}, numLowQualityBase {}", prevLowQualityIndex, numLowQualityBase);

					// the above is done only when this procedure does not remove the only solid island
					if (solidRegionsTemp.size() > 0) {
						solidRegions.clear();
						solidRegions.addAll(solidRegionsTemp);
					}
				}
			}
		}

		//--------------------------------------------------
		// STEP 0-2: remove short solid regions
		//--------------------------------------------------
		if (solidRegions.size() > 0) {
			solidRegionsTemp.clear();

			for (it_region = 0; it_region < solidRegions.size(); it_region++) {
				if ((solidRegions.get(it_region).second() - solidRegions.get(it_region).first() + 1) >= MIN_SOLID_LENGTH) {
					solidRegionsTemp.add(solidRegions.get(it_region));
				}
			}
			solidRegions.clear();
			solidRegions.addAll(solidRegionsTemp);
		}

		//--------------------------------------------------
		// STEP 0-3: remove short non-solid regions
		//--------------------------------------------------
		if (solidRegions.size() > 0) {
			solidRegionsTemp.clear();
			solidRegionsTemp.add(solidRegions.get(0));

			if (solidRegions.size() > 1) {
				for (it_region = 1; it_region < solidRegions.size(); it_region++) {
					if ((solidRegions.get(it_region).first() - solidRegions.get(it_region - 1).second() - 1) < MIN_NON_SOLID_LENGTH) {
						solidRegionsTemp.get(solidRegionsTemp.size() - 1).second(solidRegions.get(it_region).second());
					} else {
						solidRegionsTemp.add(solidRegions.get(it_region));
					}
				}
			}
			solidRegions.clear();
			solidRegions.addAll(solidRegionsTemp);
		}

		//--------------------------------------------------
		// STEP 0-4: reduce the size of solid regions
		//--------------------------------------------------
		if (solidRegions.size() > 1) {
			for (it_region = 1; it_region < solidRegions.size(); it_region++) {
				// (length of a non-solid region < kmer_length) && (length of a non-solid region >= kmer_length - FP_SUSPECT_LENGTH(default: 1))
				first = solidRegions.get(it_region).first();
				second = solidRegions.get(it_region - 1).second();
				if (((first - second - 1) < kmerLength) && ((first - second - 1) >= kmerLength - FP_SUSPECT_LENGTH)) {
					// length of the right solid region > FP_SUSPECT_LENGTH(default: 1)
					if ((solidRegions.get(it_region).second() - first + 1) > FP_SUSPECT_LENGTH) {
						solidRegions.get(it_region).first(first + FP_SUSPECT_LENGTH);
					}

					// length of the left solid region > FP_SUSPECT_LENGTH(default: 1)
					if ((second - solidRegions.get(it_region - 1).first() + 1) > FP_SUSPECT_LENGTH) {
						solidRegions.get(it_region - 1).second(second - FP_SUSPECT_LENGTH);
					}
				}
			}
		}

		//--------------------------------------------------
		// STEP 0-5: remove a solid region that makes a non-solid region shorter than k
		//--------------------------------------------------
		if (solidRegions.size() == 2) {
			// the first solid region starts from the first k-mer
			if (solidRegions.get(0).first() == 0) {
				if ((solidRegions.get(1).first() - solidRegions.get(0).second()) < (kmerLength + 1)) {
					// the distance between two regions is shorter than k

					// remove the second solid region
					solidRegions.remove(1);
				}
			} else if (solidRegions.get(1).second() == (seqLength - kmerLength)) {
				// the second solid region ends in the last k-mer

				// the distance between two regions is shorter than k
				if ((solidRegions.get(1).first() - solidRegions.get(0).second()) < (kmerLength + 1)) {
					// the length of the second solid region is >= 10% of the sequence length
					if ((solidRegions.get(1).second() - solidRegions.get(1).first() + 1) >= (seqLength * 0.1)) {
						// the length of the first solid region is < 10% of the sequence length
						if ((solidRegions.get(0).second() - solidRegions.get(0).first() + 1) < (seqLength * 0.1)) {
							// remove the first?? solid region
							solidRegions.remove(0);
						}
					}
				}
			}
		}

		//--------------------------------------------------
		// STEP 0-6: check the quality scores of right side of each solid k-mer region
		//--------------------------------------------------
		// at least one solid region
		int max_adjust, region_begin, region_end;

		if (solidRegions.size() > 0) {
			// 1 - (n - 1) solid region
			for (it_sr = 0; it_sr < (solidRegions.size() - 1); it_sr++) {
				first = solidRegions.get(it_sr).first();
				second = solidRegions.get(it_sr).second();
				max_adjust = Math.min(SOLID_REGION_ADJUST_RANGE, second - first);

				// sufficient solid regions length
				if ((second - first) >= max_adjust) {
					for (it_adjust = second; it_adjust > (second - max_adjust); it_adjust--) {
						// low quality score
						if (((quals[it_adjust + kmerLength - 1] - QS_OFFSET) < QS_CUTOFF) ||
								((quals[it_adjust] - QS_OFFSET) < QS_CUTOFF)) {
							solidRegions.get(it_sr).second(it_adjust - 1);
							break;
						}
					}
				}
			}

			for (it_sr = 1; it_sr < solidRegions.size(); it_sr++) {
				first = solidRegions.get(it_sr).first();
				second = solidRegions.get(it_sr).second();
				max_adjust = Math.min(SOLID_REGION_ADJUST_RANGE, second - first);

				if ((second - first) >= max_adjust) {
					region_begin = first;

					for (it_adjust = first; it_adjust < (region_begin + max_adjust); it_adjust++) {
						if (((quals[it_adjust] - QS_OFFSET) < QS_CUTOFF) ||
								((quals[it_adjust + kmerLength - 1] - QS_OFFSET) < QS_CUTOFF)) {
							solidRegions.get(it_sr).first(it_adjust + 1);
						}
					}
				}
			}

			// last solid region
			int index_solid_region = solidRegions.size() - 1;
			// non-solid k-mers exist at the 3-prime end
			first = solidRegions.get(index_solid_region).first();
			second = solidRegions.get(index_solid_region).second();
			max_adjust = Math.min(SOLID_REGION_ADJUST_RANGE, second - first);

			// sufficient solid regions length
			if ((second - first) >= max_adjust) {
				region_end = second;

				for (it_adjust = second; it_adjust > (region_end - max_adjust); it_adjust--) {
					// low quality score
					if ((quals[it_adjust + kmerLength - 1] - QS_OFFSET) < QS_CUTOFF)
						solidRegions.get(index_solid_region).second(it_adjust - 1);
				}
			}

			// non-solid k-mers exist at the 5-prime end
			first = solidRegions.get(0).first();
			second = solidRegions.get(0).second();
			max_adjust = Math.min(SOLID_REGION_ADJUST_RANGE, second - first);

			// sufficient solid regions length
			if ((second - first) >= max_adjust) {
				region_begin = first;

				for (it_adjust = first; it_adjust < (region_begin + max_adjust); it_adjust++) {
					// low quality score
					if ((quals[it_adjust] - QS_OFFSET) < QS_CUTOFF)
						solidRegions.get(0).first(it_adjust + 1);
				}
			}
		}

		//--------------------------------------------------
		// STEP 0-7: check whether a non-solid region < k still exists
		//--------------------------------------------------
		boolean shortNonSolidRegion = false;

		if (solidRegions.size() > 1) {
			for (it_sr = 1; it_sr < (solidRegions.size() - 1); it_sr++) {
				if ((solidRegions.get(it_sr).first() - solidRegions.get(it_sr - 1).second()) <= kmerLength) {
					shortNonSolidRegion = true;
					break;
				}
			}
		}

		if (logger.isTraceEnabled()) {
			logger.trace("{} solid regions after step 0-7 (shortNonSolidRegion {})", solidRegions.size(), shortNonSolidRegion);
			for(SolidRegion reg: solidRegions)
				logger.trace(reg.first() + " " + reg.second() + " " + (reg.second() - reg.first() + 1));
		}

		//--------------------------------------------------
		// correct errors
		//--------------------------------------------------
		byte[] basesTemp = new byte[seqLength];
		byte[] basesMod = null;

		if (logger.isTraceEnabled()) {
			basesMod = new byte[seqLength];
			Arrays.fill(basesMod, (byte) 48); //fill with zeros
		}

		if ((solidRegions.size() > 0) && (shortNonSolidRegion == false)) {
			//--------------------------------------------------
			// STEP 1-1: Correct errors between solid regions
			//--------------------------------------------------
			if (solidRegions.size() > 1) {
				// for each solid region
				for (it_region = 1; it_region < solidRegions.size(); it_region++) {
					first = solidRegions.get(it_region).first() - 1;
					second = solidRegions.get(it_region - 1).second() + 1;

					if (((first - second) + 1) >= kmerLength) {
						numCorrectedErrorsStep1_1 = correctErrorsBetweenSolidRegions(
								sequence, second, first, basesTemp, kmer, kmerRC, kmerAux,
								candidatePaths, candidatePathsTemp, basesMod);

						sumErrors += numCorrectedErrorsStep1_1;
					}
				}
			}

			//--------------------------------------------------
			// STEP 1-2: Correct errors in the 5' end
			//--------------------------------------------------
			// number of solid regions is >= 1
			if (solidRegions.size() >= 1) {
				first = solidRegions.get(0).first();
				// the first solid region does not start from the 0-th k-mer in a read
				if (first > 0) {
					numCorrectedErrorsStep1_2 = correctErrors5PrimeEnd(sequence, first - 1,
							basesTemp, kmer, kmerRC, kmerAux, candidatePaths,
							candidatePathsTemp, modificationsSequence, basesMod);

					sumErrors += numCorrectedErrorsStep1_2;
				}
			}

			//--------------------------------------------------
			// STEP 1-3: Correct errors in the 3' end
			//--------------------------------------------------
			// number of solid regions is >= 1
			if (solidRegions.size() >= 1) {
				second = solidRegions.get(solidRegions.size() - 1).second();
				// the last solid region does not end in the last k-mer in a read
				if (second < (seqLength - kmerLength)) {
					numCorrectedErrorsStep1_3 = correctErrors3PrimeEnd(sequence, second + 1,
							basesTemp, kmer, kmerRC, kmerAux, candidatePaths,
							candidatePathsTemp, modificationsSequence, basesMod);

					sumErrors += numCorrectedErrorsStep1_3;
				}
			}
		} else {
			//--------------------------------------------------
			// no solid region or short weak regions
			//--------------------------------------------------

			//--------------------------------------------------
			// STEP 2-1: Correct errors in the first k-mer
			//--------------------------------------------------
			// find potentially wrong bases
			correctErrorsFirstKmer(sequence, kmer, kmerRC, kmerAux, candidatePaths);

			// filter some candidates by extending the first k-mer to the left
			if (candidatePaths.size() > 0) {
				// each path
				for (CandidatePath path: candidatePaths) {
					if (path.getModifiedBases().size() == 0) {
						// no modified path
						candidatePathsTemp.add(path);
					} else if (path.getModifiedBases().get(0).getIndex() < (kmerLength - 1)) {
						// check the index of the first modified base
						// extension is needed
						if (solidFirstKmer(sequence, path, kmer, kmerRC, kmerAux))
							candidatePathsTemp.add(path);
					} else {
						// extension is not needed
						candidatePathsTemp.add(path);
					}
				}
			}

			if (logger.isTraceEnabled()) {
				logger.trace("candidate paths after step 2-1");
				for (CandidatePath path: candidatePathsTemp)
					logger.trace(path.getKmersQuality() + " " + path.getCoveringKmersWeight() + " " + path.getModifiedBases().size());
			}

			candidatePaths.clear();

			//--------------------------------------------------
			// STEP 2-2: extend candidate paths to the right
			//--------------------------------------------------
			if (candidatePathsTemp.size() > 0) {
				// each path
				for (CandidatePath path: candidatePathsTemp) {
					// add this path to candidate_path_vector_tmp_tmp if its correction succeeds
					CandidatePath tempPath = extendFirstKmerToRight(sequence, path, basesTemp,
							kmer, kmerRC, kmerAux, modificationsSequence);

					if (tempPath != null)
						candidatePaths.add(tempPath);
				}
			}

			if (logger.isTraceEnabled()) {
				logger.trace("candidate paths after step 2-2");
				for (CandidatePath path: candidatePaths)
					logger.trace(path.getKmersQuality() + " " + path.getCoveringKmersWeight() + " " + path.getModifiedBases().size());
			}

			//--------------------------------------------------
			// STEP 2-3: choose a final one in candidate_path_vector_tmp_tmp if possible
			//--------------------------------------------------
			// compare quality scores of candidate paths
			// if the number of paths in candidate_path_vector_tmp_tmp is larger than 1
			numCorrectedErrorsStep2 =  modifyErrorsFirstKmer(sequence, basesSafe, candidatePaths, basesMod);

			sumErrors += numCorrectedErrorsStep2;
		}

		double maxErrors = seqLength * MAX_ERROR_RATIO;

		if (sumErrors > maxErrors)
			tooManyErrors = true;

		if (tooManyErrors) {
			// Revert changes
			sequence.setBases(basesSafe);
		}

		if (logger.isTraceEnabled()) {
			logger.trace("maxErrors {}, sumErrors {}, tooManyErrors {}", maxErrors, sumErrors, tooManyErrors);	
			logger.trace("sequenceOrig {}", new String(basesSafe, StandardCharsets.US_ASCII));
			logger.trace("sequenceMod  {}", sequence.basesToString());
			logger.trace("sequenceMods {}", new String(basesMod, StandardCharsets.US_ASCII));
		}

		return sequence;
	}

	//----------------------------------------------------------------------
	// Corrects errors in a region situated between correct regions.
	//----------------------------------------------------------------------
	private int correctErrorsBetweenSolidRegions(Sequence sequence, int index_start, int index_end, byte[] basesTemp, Kmer kmer, Kmer kmerRC, Kmer kmerAux, List<CandidatePath> candidatePaths, List<CandidatePath> candidatePathsTemp, byte[] basesMod) {
		byte[] bases = sequence.getBases();
		byte kmerLength = getKmerLength();
		byte base;
		int it_alter, it_path, multi, it_check;
		int index, index_last_modified_base, nMods, num_success;
		CandidatePath candidatePath;
		boolean allSolidWoModification = false;

		candidatePaths.clear();
		candidatePathsTemp.clear();

		// index of the k-mer that can be modified
		// k-mers that are overlapped with a solid region cannot be modified
		int index_last_mod = index_end - kmerLength + 1;

		logger.trace("correctErrorsBetweenSolidRegions {},{},{}", index_start, index_end, index_last_mod);

		// make an initial k-mer
		kmer.set(bases, index_start, kmerLength);
		//logger.trace("km {}", kmer.basesToString(kmerLength));

		// for each nucleotide
		for (it_alter = Kmer.ABASE; it_alter<Kmer.NUM_NUCLEOTIDES; it_alter++) {
			// make a change
			base = (byte) it_alter;
			kmer.setBase(base, kmerLength - 1);
			//logger.trace("it_alter {} km {}", it_alter, kmer.basesToString());

			multi = getKmerMultiplicity(kmer, kmerRC);

			if (multi != 0) {
				// k-mer is solid
				logger.trace("multi {}", multi);

				index = index_start + kmerLength - 1;
				// generate a new path
				candidatePath = new CandidatePath();
				base = Kmer.DECODE[base];

				if (bases[index] != base) {
					candidatePath.addCorrection(new Correction((short)(index), base));
				}

				candidatePath.incKmersQuality(multi);
				candidatePath.incCoveringKmersWeight(COVERING_KMERS_WEIGHT);

				// if this k-mer is the last k-mer that can be modified
				// running extend_a_kmer_right is not needed any more
				if (index_start == index_last_mod) {
					candidatePathsTemp.add(candidatePath);
				} else {
					// trace  this kmer recursively and update candidatePathsTemp
					extendKmer(kmer, index_start, index_last_mod, sequence, kmerRC, 
							kmerAux, candidatePath, candidatePathsTemp);
				}
			}
		}

		// check the solidness of k-mers between index_last_mod and index_end
		allSolidWoModification = false;
		List<Correction> modifiedBasesList;

		// for each candidate path
		for (it_path = 0; it_path < candidatePathsTemp.size(); it_path++) {
			candidatePath = candidatePathsTemp.get(it_path);
			logger.trace(candidatePath.getKmersQuality() + " " + candidatePath.getCoveringKmersWeight() + " " + candidatePath.getModifiedBases().size());

			if (candidatePath.getModifiedBases().size() == 0) {
				allSolidWoModification = true;
				break;
			} else {
				// checking is needed
				modifiedBasesList = candidatePath.getModifiedBases();
				nMods = modifiedBasesList.size() - 1;
				index_last_modified_base = modifiedBasesList.get(nMods).getIndex();

				logger.trace("index_last_modified_base, index_last_mod {},{}", index_last_modified_base, index_last_mod);

				if (index_last_modified_base > index_last_mod) {
					// generate a temporary sequence
					System.arraycopy(bases, 0, basesTemp, 0, bases.length);
					for(Correction corr: modifiedBasesList)
						basesTemp[corr.getIndex()] = corr.getBase();

					//logger.trace("sequence_tmp {}", new String(basesTemp, StandardCharsets.US_ASCII));

					// check k-mers
					num_success = 0;
					for (it_check = index_last_mod; it_check <= index_last_modified_base; it_check++) {
						kmer.set(basesTemp, it_check, kmerLength);
						//logger.trace("kmer_current {}", kmer.basesToString(kmerLength));

						multi = getKmerMultiplicity(kmer, kmerRC);

						if (multi != 0) {
							// k-mer is solid
							logger.trace("multi {}", multi);
							num_success++;
						} else {
							break;
						}
					}

					logger.trace("num_success {}", num_success);

					if (num_success == (index_last_modified_base - index_last_mod + 1))
						candidatePaths.add(candidatePath);
				} else {
					// checking is not needed
					candidatePaths.add(candidatePath);
				}
			}
		}

		if (logger.isTraceEnabled()) {
			logger.trace("allSolidWoModification {}", allSolidWoModification);
			logger.trace("candidatePaths size {}", candidatePaths.size());
			for (CandidatePath path: candidatePaths)
				logger.trace(path.getKmersQuality() + " " + path.getCoveringKmersWeight() + " " + path.getModifiedBases().size());
		}

		// all k-mers are solid without any modification
		if (allSolidWoModification == true)
			// do nothing
			return 0;

		// compare quality scores of candidate paths
		// if the number of paths in candidate_path_vector is larger than 1
		return modifyErrors(sequence, candidatePaths, basesMod);
	}

	//----------------------------------------------------------------------
	// Corrects errors situated on a 5' end of the read
	//----------------------------------------------------------------------
	private int correctErrors5PrimeEnd(Sequence sequence, int index_start, byte[] basesTemp, Kmer kmer, Kmer kmerRC, Kmer kmerAux, List<CandidatePath> candidatePaths, List<CandidatePath> candidatePathsTemp, List<MutablePair<Byte,Integer>> modificationsSequence, byte[] basesMod) {
		byte[] bases = sequence.getBases();
		byte kmerLength = getKmerLength();
		byte base;
		int it_alter, it_path, multi, max_remaining_changes, checked_changes;
		CandidatePath candidatePath;

		logger.trace("correctErrors5PrimeEnd {}", index_start);

		candidatePaths.clear();
		candidatePathsTemp.clear();

		// make an initial k-mer
		kmer.set(bases, index_start, kmerLength);
		//logger.trace("km {}", kmer.basesToString(kmerLength));

		// for each nucleotide
		for (it_alter = Kmer.ABASE; it_alter<Kmer.NUM_NUCLEOTIDES; it_alter++) {
			// make a change
			kmer.setBase((byte) it_alter, 0);
			//logger.trace("it_alter {} km {}", it_alter, kmer.basesToString(kmerLength));

			multi = getKmerMultiplicity(kmer, kmerRC);

			if (multi != 0) {
				// k-mer is solid
				logger.trace("multi {}", multi);

				base = Kmer.DECODE[(byte) it_alter];

				// if this k-mer is the first k-mer in a read
				// running extend_a_kmer_5_prime_end is not needed any more
				if (index_start == 0) {
					// generate a new path
					candidatePath = new CandidatePath();
					candidatePath.incKmersQuality(multi);
					candidatePath.incCoveringKmersWeight(COVERING_KMERS_WEIGHT);
					candidatePath.addCorrection(new Correction((short)(index_start), base));
					candidatePathsTemp.add(candidatePath);
				} else if (index_start > 0) {
					if (modificationsSequence.size() == 0)
						modificationsSequence.add(new MutablePair<Byte,Integer>(base, multi));
					else
						modificationsSequence.set(0, new MutablePair<Byte,Integer>(base, multi));

					if (bases[index_start] == base)
						max_remaining_changes = (int)(MAX_CHANGES_IN_REGION_RATIO * index_start);
					else
						max_remaining_changes = (int)((MAX_CHANGES_IN_REGION_RATIO * index_start) - 1);

					logger.trace("modificationsSequence[0].base {}, max_remaining_changes {}", base, max_remaining_changes);

					// trace this kmer recursively and update candidate_path_vector_tmp
					checked_changes = 0;
					extendKmer5PrimeEnd(kmer, index_start, sequence, kmerRC, kmerAux, 
							candidatePathsTemp, max_remaining_changes, 1,
							checked_changes, modificationsSequence);
				}
			}
		}

		// for each candidate path
		for (it_path = 0; it_path < candidatePathsTemp.size(); it_path++) {
			candidatePath = candidatePathsTemp.get(it_path);
			logger.trace("extending " + candidatePath.getKmersQuality() + " " + candidatePath.getCoveringKmersWeight() + " " + candidatePath.getModifiedBases().size());
			performExtendOutLeft(kmer, sequence, basesTemp, candidatePath, candidatePaths, kmerRC, kmerAux);
		}

		if (logger.isTraceEnabled()) {
			logger.trace("candidatePaths size {}", candidatePaths.size());
			for (CandidatePath path: candidatePaths)
				logger.trace(path.getKmersQuality() + " " + path.getCoveringKmersWeight() + " " + path.getModifiedBases().size());
		}

		return modifyErrors(sequence, candidatePaths, basesMod);
	}

	//----------------------------------------------------------------------
	// Corrects errors situated on a 3' end of the read
	//----------------------------------------------------------------------
	private int correctErrors3PrimeEnd(Sequence sequence, int index_start, byte[] basesTemp, Kmer kmer, Kmer kmerRC, Kmer kmerAux, List<CandidatePath> candidatePaths, List<CandidatePath> candidatePathsTemp, List<MutablePair<Byte,Integer>> modificationsSequence, byte[] basesMod) {
		byte[] bases = sequence.getBases();
		int seqLength = sequence.getLength();
		byte kmerLength = getKmerLength();
		int size = seqLength - kmerLength;
		byte base;
		int it_alter, it_path, multi, index, max_remaining_changes, checked_changes;
		CandidatePath candidatePath;

		logger.trace("correctErrors3PrimeEnd {}", index_start);

		candidatePaths.clear();
		candidatePathsTemp.clear();

		// make an initial k-mer
		kmer.set(bases, index_start, kmerLength);
		//logger.trace("km {}", kmer.basesToString(kmerLength));

		// for each nucleotide
		for (it_alter = Kmer.ABASE; it_alter<Kmer.NUM_NUCLEOTIDES; it_alter++) {
			// make a change
			kmer.setBase((byte) it_alter, kmerLength - 1);
			//logger.trace("it_alter {} km {}", it_alter, kmer.basesToString(kmerLength));

			multi = getKmerMultiplicity(kmer, kmerRC);

			if (multi != 0) {
				// k-mer is solid
				logger.trace("multi {}", multi);

				base = Kmer.DECODE[(byte) it_alter];

				// if this k-mer is the last k-mer in a read
				// running extend_a_kmer_3_prime_end is not needed any more
				if (index_start == size) {
					// generate a new path
					candidatePath = new CandidatePath();
					candidatePath.incKmersQuality(multi);
					candidatePath.incCoveringKmersWeight(COVERING_KMERS_WEIGHT);
					index = index_start + kmerLength - 1;
					candidatePath.addCorrection(new Correction((short)(index), base));
					candidatePathsTemp.add(candidatePath);
				} else if (index_start < size) {
					if (modificationsSequence.size() == 0)
						modificationsSequence.add(new MutablePair<Byte,Integer>(base, multi));
					else
						modificationsSequence.set(0, new MutablePair<Byte,Integer>(base, multi));

					if (bases[index_start] == base)
						max_remaining_changes = (int)(MAX_CHANGES_IN_REGION_RATIO * (seqLength - index_start));
					else
						max_remaining_changes = (int)((MAX_CHANGES_IN_REGION_RATIO * (seqLength - index_start)) - 1);

					logger.trace("modificationsSequence[0].base {, max_remaining_changes {}}", base, max_remaining_changes);

					// trace this kmer recursively and update candidate_path_vector_tmp
					checked_changes = 0;
					extendKmer3PrimeEnd(kmer, index_start, sequence, kmerRC, kmerAux, 
							new CandidatePath(), candidatePathsTemp, max_remaining_changes, 1,
							checked_changes, modificationsSequence);
				}
			}
		}

		// for each candidate path
		for (it_path = 0; it_path < candidatePathsTemp.size(); it_path++) {
			candidatePath = candidatePathsTemp.get(it_path);
			logger.trace("extending " +candidatePath.getKmersQuality() + " " + candidatePath.getCoveringKmersWeight() + " " + candidatePath.getModifiedBases().size());
			performExtendOutRight(kmer, sequence, basesTemp, candidatePath, candidatePaths, kmerRC, kmerAux);
		}

		if (logger.isTraceEnabled()) {
			logger.trace("candidatePaths size {}", candidatePaths.size());
			for (CandidatePath path: candidatePaths)
				logger.trace(path.getKmersQuality() + " " + path.getCoveringKmersWeight() + " " + path.getModifiedBases().size());
		}

		return modifyErrors(sequence, candidatePaths, basesMod);
	}

	//----------------------------------------------------------------------
	// Corrects errors in the first k-mer of the read.
	//----------------------------------------------------------------------
	private void correctErrorsFirstKmer(Sequence sequence, Kmer firstKmer, Kmer kmerRC, Kmer kmerAux, List<CandidatePath> candidatePaths) {
		byte[] sequenceModified = sequence.getBases();
		byte[] quals = sequence.getQuals();
		byte kmerLength = getKmerLength();
		byte base, firstKmerBase;
		int i, it_alter, it_bases, it_qualities, it_mod, multi;
		CandidatePath candidatePath;
		List<Short> lowQsIndexes = new ArrayList<Short>();
		List<Short> candidates;
		List<MutablePair<Byte,Short>> qualities;
		List<MutablePair<Double,CandidatePath>> rates;

		candidatePaths.clear();

		// make an initial k-mer
		firstKmer.set(sequenceModified, 0, kmerLength);
		//logger.trace("km {}", firstKmer.basesToString(kmerLength));

		for (it_bases = 0; it_bases < kmerLength; it_bases++) {
			if ((quals[it_bases] - QS_OFFSET) < QS_CUTOFF) {
				lowQsIndexes.add((short)it_bases);
			}
		}

		if (logger.isTraceEnabled()) {
			logger.trace("correctErrorsFirstKmer: lowQsIndexes size {}", lowQsIndexes.size());
			for (Short index: lowQsIndexes)
				logger.trace("lowQsIndex {}, quality {}", index, quals[index]);
		}

		if ((lowQsIndexes.size() <= MAX_LOW_QS_BASES) && (lowQsIndexes.size() > 0)) {
			// correct errors if the number of low-quality bases is smaller than the threshold
			logger.trace("low-quality bases is smaller than MAX_LOW_QS_BASES {}", MAX_LOW_QS_BASES);

			byte[] candidateFastPath = new byte[MAX_CHECK_FIRST_KMER_NESTING];
			kmerAux.set(firstKmer);

			checkFirstKmer(kmerAux, kmerRC, candidateFastPath, lowQsIndexes, candidatePaths, 0);

			logger.trace("candidatePaths size {}", candidatePaths.size());

			// no candidate path is found
			if (candidatePaths.size() == 0) {
				for (it_bases = 0; it_bases < kmerLength; it_bases++) {
					firstKmerBase = firstKmer.getBase(it_bases);
					kmerAux.set(firstKmer);

					// for each nucleotide
					for (it_alter = Kmer.ABASE; it_alter<Kmer.NUM_NUCLEOTIDES; it_alter++) {
						base = (byte) it_alter;

						if (firstKmerBase != base) {
							// not equal to the original character
							// generate a new k-mer
							kmerAux.setBase(base, it_bases);
							//logger.trace("kmerAux {}", kmerAux.basesToString());

							// add kmer_tmp to candidate_path_tmp if it is solid
							multi = getKmerMultiplicity(kmerAux, kmerRC);

							if (multi != 0) {
								// k-mer is solid
								logger.trace("multi {}", multi);
								// generate a new candidate path
								base = Kmer.DECODE[base];
								candidatePath = new CandidatePath();
								candidatePath.incKmersQuality(multi);
								candidatePath.incCoveringKmersWeight(COVERING_KMERS_WEIGHT);
								candidatePath.addCorrection(new Correction((short)(it_bases), base));
								candidatePaths.add(candidatePath);
							}
						}
					}
				}
			}
		} else {
			// no low-quality base or too many low-quality bases
			multi = getKmerMultiplicity(firstKmer, kmerRC);

			if (multi != 0) {
				// k-mer is solid
				logger.trace("multi {}", multi);
				// generate a new path
				candidatePath = new CandidatePath();
				candidatePath.incKmersQuality(multi);
				candidatePath.incCoveringKmersWeight(COVERING_KMERS_WEIGHT);
				candidatePaths.add(candidatePath);
			} else {
				// (quality, position)
				qualities = new ArrayList<MutablePair<Byte,Short>>(kmerLength);

				for (it_qualities = 0; it_qualities < kmerLength; it_qualities++)
					qualities.add(new MutablePair<Byte,Short>(quals[it_qualities], (short) it_qualities));

				// Sort list by quality
				qualities.sort(new Comparator<MutablePair<Byte,Short>>() {
					@Override
					public int compare(MutablePair<Byte,Short> p1, MutablePair<Byte,Short> p2) {
						return p1.getLeft().compareTo(p2.getLeft());
					}
				});

				if (logger.isTraceEnabled()) {
					logger.trace("qualities size {}", qualities.size());
					for (MutablePair<Byte,Short> qual: qualities)
						logger.trace("{}, {}", qual.right, qual.left);
				}

				if (lowQsIndexes.size() == 0) {
					for (i = 0; i < kmerLength; i++) {
						it_bases = qualities.get(i).right;
						firstKmerBase = firstKmer.getBase(it_bases);
						kmerAux.set(firstKmer);

						// for each nucleotide
						for (it_alter = Kmer.ABASE; it_alter<Kmer.NUM_NUCLEOTIDES; it_alter++) {
							base = (byte) it_alter;

							if (firstKmerBase != base) {
								// not equal to the original character
								// generate a new k-mer
								kmerAux.setBase(base, it_bases);
								//logger.trace("kmerAux {}", kmerAux.basesToString());

								// add kmer_tmp to candidate_path_tmp if it is solid
								multi = getKmerMultiplicity(kmerAux, kmerRC);

								if (multi != 0) {
									// k-mer is solid
									logger.trace("multi {}", multi);
									// generate a new candidate path
									base = Kmer.DECODE[base];
									candidatePath = new CandidatePath();
									candidatePath.incKmersQuality(multi);
									candidatePath.incCoveringKmersWeight(COVERING_KMERS_WEIGHT);
									candidatePath.addCorrection(new Correction((short)(it_bases), base));
									candidatePaths.add(candidatePath);
								}
							}
						}
					}
				} else {
					candidates = new ArrayList<Short>();
					int min = Math.min(lowQsIndexes.size(), MAX_LOW_QS_INDEXES_COMB);

					for (i = 0; i < min; i++)
						candidates.add(qualities.get(i).right);

					if (logger.isTraceEnabled()) {
						logger.trace("candidates size {}", candidates.size());
						for (Short index: candidates)
							logger.trace("candidate {}", index);
					}

					if (candidates.size() > 0) {
						byte[] candidateFastPath = new byte[MAX_CHECK_FIRST_KMER_NESTING];
						kmerAux.set(firstKmer);

						checkFirstKmer(kmerAux, kmerRC, candidateFastPath, candidates,
								candidatePaths, 0);
					}
				}
			}
		}

		if (logger.isTraceEnabled()) {
			logger.trace("candidatePaths size {}", candidatePaths.size());
			for (CandidatePath path: candidatePaths)
				logger.trace(path.getKmersQuality() + " " + path.getCoveringKmersWeight() + " " + path.getModifiedBases().size());
		}

		if (candidatePaths.size() > MAX_FIRST_KMER_POSSIBILITIES) {
			// (rate, path)
			rates = new ArrayList<MutablePair<Double,CandidatePath>>();
			List<Correction> modifiedBases;
			double rate;

			// each candidate path
			for (CandidatePath path: candidatePaths) {
				// each modification
				rate = path.getCoveringKmersWeight();
				modifiedBases = path.getModifiedBases();

				for (it_mod = 0; it_mod < modifiedBases.size(); it_mod++) {
					// multiply bases' error probabilities
					base = modifiedBases.get(it_mod).getBase();
					i = modifiedBases.get(it_mod).getIndex();

					if (sequenceModified[i] != base)
						rate *= convertQuality2Probability(quals[i]);
				}

				rates.add(new MutablePair<Double,CandidatePath>(rate, path));
			}

			// Sort list by rate
			rates.sort(new Comparator<MutablePair<Double,CandidatePath>>() {
				@Override
				public int compare(MutablePair<Double,CandidatePath> p1, MutablePair<Double,CandidatePath> p2) {
					return p1.getLeft().compareTo(p2.getLeft());
				}
			});

			if (logger.isTraceEnabled()) {
				logger.trace("rates size {}", rates.size());
				for (MutablePair<Double,CandidatePath> pair: rates) {
					logger.trace("rate {}", pair.left);
					logger.trace(pair.right.getKmersQuality() + " " + pair.right.getCoveringKmersWeight() + " " + pair.right.getModifiedBases().size());
				}
			}

			candidatePaths.clear();
			for (i = 0; i < MAX_FIRST_KMER_POSSIBILITIES; i++)
				candidatePaths.add(rates.get(rates.size() - i - 1).right);
		}

		if (logger.isTraceEnabled()) {
			logger.trace("final candidatePaths size {}", candidatePaths.size());
			for (CandidatePath path: candidatePaths)
				logger.trace(path.getKmersQuality() + " " + path.getCoveringKmersWeight() + " " + path.getModifiedBases().size());
		}
	}

	//----------------------------------------------------------------------
	// Tries to correct k-mer by changing one symbol.
	//----------------------------------------------------------------------
	private void checkFirstKmer(Kmer kmer, Kmer kmerRC, byte[] candidatePath, List<Short> candidatesIndexes, List<CandidatePath> candidatePaths, int index) {
		int it_alter, multi;
		byte base;
		CandidatePath pathTemp;

		logger.trace("checkFirstKmer: index {}", index);

		if (candidatePaths.size() >= MAX_FIRST_KMER_CORRECTION_PATHS)
			return;

		// for each nucleotide
		for (it_alter = Kmer.ABASE; it_alter<Kmer.NUM_NUCLEOTIDES; it_alter++) {
			// make a new k-mer
			base = (byte) it_alter;
			kmer.setBase(base, candidatesIndexes.get(index));			
			candidatePath[index] = Kmer.DECODE[base];
			//logger.trace("kmer {}", kmer.basesToString());

			if (index == candidatesIndexes.size() - 1) {
				multi = getKmerMultiplicity(kmer, kmerRC);

				if (multi != 0) {
					// k-mer is solid
					logger.trace("multi {}", multi);
					// generate a new path
					pathTemp = new CandidatePath(multi, COVERING_KMERS_WEIGHT, candidatesIndexes, candidatePath);
					candidatePaths.add(pathTemp);
				}
			} else {
				checkFirstKmer(kmer, kmerRC, candidatePath, candidatesIndexes, 
						candidatePaths, index + 1);
			}
		}
	}

	//----------------------------------------------------------------------
	// Modifies read until reaches a specified position.
	//----------------------------------------------------------------------
	private void extendKmer(Kmer kmer, int index_kmer, int index_last_mod, Sequence sequence, Kmer kmerRC, Kmer kmerAux, CandidatePath currentPath, List<CandidatePath> candidatePaths) {
		byte[] sequenceModified = sequence.getBases();
		byte kmerLength = getKmerLength();
		byte base;
		int multi, it_alter, index = index_kmer + kmerLength;

		logger.trace("extendKmer: index_kmer {}, index_last_mod {}", index_kmer, index_last_mod);

		// generate a new k-mer
		kmerAux.set(kmer);
		kmerAux.forwardBase(sequenceModified[index], kmerLength);
		//logger.trace("kmer {}", kmerAux.basesToString());

		multi = getKmerMultiplicity(kmerAux, kmerRC);

		if (multi != 0) {
			// k-mer is solid
			logger.trace("kmer is solid, multi {}", multi);

			currentPath.incKmersQuality(multi);
			currentPath.incCoveringKmersWeight(COVERING_KMERS_WEIGHT);

			// if this k-mer is the last k-mer that can be modified
			// running extend_a_kmer_right is not needed any more
			if ((index_kmer + 1) == index_last_mod) {
				candidatePaths.add(currentPath);
			} else {
				extendKmer(kmerAux, index_kmer + 1, index_last_mod, sequence,
						kmerRC, KmerGenerator.createKmer(), currentPath,
						candidatePaths);
			}
		} else {
			logger.trace("kmer is not solid");

			// for each nucleotide
			for (it_alter = Kmer.ABASE; it_alter<Kmer.NUM_NUCLEOTIDES; it_alter++) {
				base = Kmer.DECODE[it_alter];

				// not equal to the original character
				if (sequenceModified[index] != base) {
					// make a change
					kmerAux.setBase((byte) it_alter, kmerLength - 1);
					//logger.trace("kmer {}", kmerAux.basesToString());

					multi = getKmerMultiplicity(kmerAux, kmerRC);

					if (multi != 0) {
						// k-mer is solid
						logger.trace("kmer is solid, multi {}", multi);

						// generate a new path
						CandidatePath tempPath = new CandidatePath(currentPath);
						tempPath.incKmersQuality(multi);
						tempPath.incCoveringKmersWeight(COVERING_KMERS_WEIGHT);
						tempPath.addCorrection(new Correction((short)(index), base));

						if (logger.isTraceEnabled()) {
							logger.trace("corrections after adding tempPath");
							for (Correction corr: tempPath.getModifiedBases()) {
								logger.trace("{} {}", corr.getIndex(), corr.getBase());
							}
						}

						// if this k-mer is the last k-mer that can be modified
						// running extend_a_kmer_right is not needed any more
						if ((index_kmer + 1) == index_last_mod) {
							candidatePaths.add(tempPath);
						} else {
							extendKmer(kmerAux, index_kmer + 1, index_last_mod, sequence,
									kmerRC, KmerGenerator.createKmer(),
									tempPath, candidatePaths);
						}
					}
				}
			}
		}
	}

	//----------------------------------------------------------------------
	// Proceeds correction towards 5' end.
	//----------------------------------------------------------------------
	private void extendKmer5PrimeEnd(Kmer kmer, int index_kmer, Sequence sequence, Kmer kmerRC, Kmer kmerAux, List<CandidatePath> candidatePaths, int max_remaining_changes, int nesting, int checked_changes, List<MutablePair<Byte,Integer>> modificationsSequence) {
		byte[] sequenceModified = sequence.getBases();
		byte[] quals = sequence.getQuals();
		byte kmerLength = getKmerLength();
		byte base, baseTemp;
		int multi = 0, it, it_alter, index;
		boolean isLowQualityBase = false;

		logger.trace("extendKmer5PrimeEnd: index_kmer {}, nesting {}, max_remaining_changes {}", index_kmer, nesting, max_remaining_changes);

		if (candidatePaths.size() > MAX_EXTEND_CORRECTION_PATHS)
			return;

		// generate a new k-mer
		base = sequenceModified[index_kmer - 1];
		kmerAux.set(kmer);
		kmerAux.backwardBase(base, kmerLength);
		//logger.trace("kmer {}", kmerAux.basesToString(kmerLength));

		isLowQualityBase = ((quals[index_kmer - 1] - QS_OFFSET) < QS_CUTOFF);

		if (!isLowQualityBase) {
			multi = getKmerMultiplicity(kmerAux, kmerRC);

			if (multi != 0) {
				// k-mer is solid
				logger.trace("kmer is solid, multi {}", multi);

				if (modificationsSequence.size() <= nesting)
					modificationsSequence.add(new MutablePair<Byte,Integer>(base, multi));
				else
					modificationsSequence.set(nesting, new MutablePair<Byte,Integer>(base, multi));

				logger.trace("modificationsSequence[{}].base {}", nesting, base);

				// if this k-mer is the first k-mer in a read
				// running extend_a_kmer_5_prime_end is not needed any more
				if ((index_kmer - 1) == 0) {
					// generate a new path
					CandidatePath candidatePath = new CandidatePath();

					for (it = 0; it <= nesting; it++) {
						index = nesting - it;
						baseTemp = modificationsSequence.get(it).getLeft();

						if (baseTemp != sequenceModified[index]) {
							candidatePath.addCorrection(new Correction((short) index, baseTemp));
						}
						candidatePath.incKmersQuality(modificationsSequence.get(it).getRight());
						candidatePath.incCoveringKmersWeight(COVERING_KMERS_WEIGHT);
					}

					candidatePaths.add(candidatePath);

					if (logger.isTraceEnabled()) {
						logger.trace("candidatePath corrections");
						for (Correction corr: candidatePath.getModifiedBases()) {
							logger.trace("{} {}", corr.getIndex(), corr.getBase());
						}
					}
				} else if ((index_kmer - 1) > 0) {
					extendKmer5PrimeEnd(kmerAux, index_kmer - 1, sequence, kmerRC,
							KmerGenerator.createKmer(), candidatePaths,
							max_remaining_changes, nesting + 1,
							checked_changes, modificationsSequence);
				}
			}
		}

		if ((!isLowQualityBase && multi == 0) || isLowQualityBase) {
			logger.trace("kmer is not solid or isLowQualityBase {}", isLowQualityBase);

			// for each nucleotide
			for (it_alter = Kmer.ABASE; it_alter<Kmer.NUM_NUCLEOTIDES; it_alter++) {
				baseTemp = Kmer.DECODE[it_alter];

				// not equal to the original character
				if (base != baseTemp || isLowQualityBase) {
					// make a change
					kmerAux.setBase((byte) it_alter, 0);
					//logger.trace("kmer {}", kmerAux.basesToString());

					multi = getKmerMultiplicity(kmerAux, kmerRC);

					if (multi != 0) {
						// k-mer is solid
						logger.trace("kmer is solid, multi {}", multi);

						if (modificationsSequence.size() <= nesting)
							modificationsSequence.add(new MutablePair<Byte,Integer>(baseTemp, multi));
						else
							modificationsSequence.set(nesting, new MutablePair<Byte,Integer>(baseTemp, multi));

						logger.trace("modificationsSequence[{}].base {}", nesting, baseTemp);

						// if this k-mer is the first k-mer in a read
						// running extend_a_kmer_5_prime_end is not needed any more
						if ((index_kmer - 1) == 0) {
							// generate a new path
							CandidatePath candidatePath = new CandidatePath();

							for (it = 0; it <= nesting; it++) {
								index = nesting - it;
								baseTemp = modificationsSequence.get(it).getLeft();

								if (baseTemp != sequenceModified[index]) {
									candidatePath.addCorrection(new Correction((short) index, baseTemp));
								}
								candidatePath.incKmersQuality(modificationsSequence.get(it).getRight());
								candidatePath.incCoveringKmersWeight(COVERING_KMERS_WEIGHT);
							}

							candidatePaths.add(candidatePath);

							if (logger.isTraceEnabled()) {
								logger.trace("candidatePath corrections");
								for (Correction corr: candidatePath.getModifiedBases()) {
									logger.trace("{} {}", corr.getIndex(), corr.getBase());
								}
							}
						} else if ((index_kmer - 1) > 0) {

							if (LIMIT_MODIFICATIONS && (max_remaining_changes <= 1 && base == baseTemp))
								continue;

							checked_changes++;
							logger.trace("checked_changes {}", checked_changes);
							if (checked_changes > CHECK_MAX_CHANGES)
								return;

							if (base != baseTemp || isLowQualityBase)
								max_remaining_changes = max_remaining_changes - 1;

							logger.trace("max_remaining_changes {}", max_remaining_changes);

							extendKmer5PrimeEnd(kmerAux, index_kmer - 1, sequence, kmerRC,
									KmerGenerator.createKmer(), candidatePaths,
									max_remaining_changes, nesting + 1,
									checked_changes, modificationsSequence);
						}
					}
				}
			}
		}
	}

	//----------------------------------------------------------------------
	// Proceeds correction towards 3' end.
	//----------------------------------------------------------------------
	private void extendKmer3PrimeEnd(Kmer kmer, int index_kmer, Sequence sequence, Kmer kmerRC, Kmer kmerAux, CandidatePath pathIn, List<CandidatePath> candidatePaths, int max_remaining_changes, int nesting, int checked_changes, List<MutablePair<Byte,Integer>> modificationsSequence) {
		byte[] sequenceModified = sequence.getBases();
		byte[] quals = sequence.getQuals();
		byte kmerLength = getKmerLength();
		int seqLength = sequence.getLength();
		int size = seqLength - kmerLength;
		byte base, baseTemp;
		int multi = 0, it, it_alter, index;
		boolean isLowQualityBase = false;

		logger.trace("extendKmer3PrimeEnd: index_kmer {}, nesting {}, max_remaining_changes {}", index_kmer, nesting, max_remaining_changes);

		if (candidatePaths.size() > MAX_EXTEND_CORRECTION_PATHS)
			return;

		// generate a new k-mer
		base = sequenceModified[index_kmer + kmerLength];
		kmerAux.set(kmer);
		kmerAux.forwardBase(base, kmerLength);
		//logger.trace("kmer {}", kmerAux.basesToString(kmerLength));

		isLowQualityBase = ((quals[index_kmer + kmerLength] - QS_OFFSET) < QS_CUTOFF);

		if (!isLowQualityBase) {
			multi = getKmerMultiplicity(kmerAux, kmerRC);

			if (multi != 0) {
				// k-mer is solid
				logger.trace("kmer is solid, multi {}", multi);

				if (modificationsSequence.size() <= nesting)
					modificationsSequence.add(new MutablePair<Byte,Integer>(base, multi));
				else
					modificationsSequence.set(nesting, new MutablePair<Byte,Integer>(base, multi));

				logger.trace("modificationsSequence[{}].base {}", nesting, base);

				// if this k-mer is the last k-mer in a read
				// running extend_a_kmer_3_prime_end is not needed any more
				if ((index_kmer + 1) == size) {
					// generate a new path
					CandidatePath candidatePath = new CandidatePath(pathIn);

					for (it = 0; it <= nesting; it++) {
						index = seqLength - nesting + it - 1;
						baseTemp = modificationsSequence.get(it).getLeft();

						if (baseTemp != sequenceModified[index]) {
							candidatePath.addCorrection(new Correction((short) index, baseTemp));
						}
						candidatePath.incKmersQuality(modificationsSequence.get(it).getRight());
						candidatePath.incCoveringKmersWeight(COVERING_KMERS_WEIGHT);
					}

					candidatePaths.add(candidatePath);

					if (logger.isTraceEnabled()) {
						logger.trace("candidatePath corrections");
						for (Correction corr: candidatePath.getModifiedBases()) {
							logger.trace("{} {}", corr.getIndex(), corr.getBase());
						}
					}
				} else if ((index_kmer + 1) < size) {
					extendKmer3PrimeEnd(kmerAux, index_kmer + 1, sequence, kmerRC,
							KmerGenerator.createKmer(), pathIn, candidatePaths,
							max_remaining_changes, nesting + 1,
							checked_changes, modificationsSequence);
				}
			}
		}

		if ((!isLowQualityBase && multi == 0) || isLowQualityBase) {
			logger.trace("kmer is not solid or isLowQualityBase {}", isLowQualityBase);

			// for each nucleotide
			for (it_alter = Kmer.ABASE; it_alter<Kmer.NUM_NUCLEOTIDES; it_alter++) {
				baseTemp = Kmer.DECODE[it_alter];

				// not equal to the original character
				if (base != baseTemp || isLowQualityBase) {
					// make a change
					kmerAux.setBase((byte) it_alter, kmerLength - 1);
					//logger.trace("kmer {}", kmerAux.basesToString(kmerLength));

					multi = getKmerMultiplicity(kmerAux, kmerRC);

					if (multi != 0) {
						// k-mer is solid
						logger.trace("kmer is solid, multi {}", multi);

						if (modificationsSequence.size() <= nesting)
							modificationsSequence.add(new MutablePair<Byte,Integer>(baseTemp, multi));
						else
							modificationsSequence.set(nesting, new MutablePair<Byte,Integer>(baseTemp, multi));

						logger.trace("modificationsSequence[{}].base {}", nesting, baseTemp);

						// if this k-mer is the last k-mer in a read
						// running extend_a_kmer_3_prime_end is not needed any more
						if ((index_kmer + 1) == size) {
							// generate a new path
							CandidatePath candidatePath = new CandidatePath(pathIn);

							for (it = 0; it <= nesting; it++) {
								index = seqLength - nesting + it - 1;
								baseTemp = modificationsSequence.get(it).getLeft();

								if (baseTemp != sequenceModified[index]) {
									candidatePath.addCorrection(new Correction((short) index, baseTemp));
								}
								candidatePath.incKmersQuality(modificationsSequence.get(it).getRight());
								candidatePath.incCoveringKmersWeight(COVERING_KMERS_WEIGHT);
							}

							candidatePaths.add(candidatePath);

							if (logger.isTraceEnabled()) {
								logger.trace("candidatePath corrections");
								for (Correction corr: candidatePath.getModifiedBases()) {
									logger.trace("{} {}", corr.getIndex(), corr.getBase());
								}
							}
						} else if ((index_kmer + 1) < size) {

							if (LIMIT_MODIFICATIONS && (max_remaining_changes <= 1 && sequenceModified[index_kmer - 1] == baseTemp))
								continue;

							checked_changes++;
							logger.trace("checked_changes {}", checked_changes);
							if (checked_changes > CHECK_MAX_CHANGES)
								return;

							if (sequenceModified[index_kmer - 1] != baseTemp)
								max_remaining_changes = max_remaining_changes - 1;

							logger.trace("max_remaining_changes {}", max_remaining_changes);

							extendKmer3PrimeEnd(kmerAux, index_kmer + 1, sequence, kmerRC,
									KmerGenerator.createKmer(), pathIn, candidatePaths,
									max_remaining_changes, nesting + 1,
									checked_changes, modificationsSequence);
						}
					}
				}
			}
		}
	}

	//----------------------------------------------------------------------
	// Checks if the read can be extended towards 5'.
	//----------------------------------------------------------------------
	private void performExtendOutLeft(Kmer kmer, Sequence sequence, byte[] sequence_tmp, CandidatePath candidatePath, List<CandidatePath> candidatePaths, Kmer kmerRC, Kmer kmerAux) {
		int index_smallest_modified = 0, extend_amount, it_alter;
		byte kmerLength = getKmerLength();
		List<Correction> modifiedBases = candidatePath.getModifiedBases();
		boolean extensionSuccess = false;
		int multi, max_extended_kmer_quality = 0;

		if (modifiedBases.size() > 0)
			index_smallest_modified = modifiedBases.get(modifiedBases.size() - 1).getIndex();

		logger.trace("performExtendOutLeft: index_smallest_modified {}", index_smallest_modified);

		if (index_smallest_modified >= kmerLength - 1) {
			candidatePaths.add(candidatePath);
			return;
		}

		// extension is needed

		// generate a temporary sequence
		System.arraycopy(sequence.getBases(), 0, sequence_tmp, 0, sequence_tmp.length);

		// applied the modified bases to sequence_tmp
		for(Correction corr: modifiedBases)
			// modify sequence_tmp
			sequence_tmp[corr.getIndex()] = corr.getBase();

		//logger.trace("sequence_tmp {}", new String(sequence_tmp, StandardCharsets.US_ASCII));

		if (index_smallest_modified >= kmerLength - MAX_EXTEND - 1) {
			extend_amount = kmerLength - index_smallest_modified - 1;
		} else {
			extend_amount = MAX_EXTEND;
		}

		logger.trace("extend_amount {}", extend_amount);

		// generate an initial k-mer
		kmer.set(sequence_tmp, 0, kmerLength);
		kmer.backwardBase((byte)'N', kmerLength);

		// for each nucleotide
		for (it_alter = Kmer.ABASE; it_alter<Kmer.NUM_NUCLEOTIDES; it_alter++) {
			// make a change
			kmer.setBase((byte) it_alter, 0);
			//logger.trace("kmer {}", kmer.basesToString());
			multi = getKmerMultiplicity(kmer, kmerRC);

			if (multi != 0) {
				// k-mer is solid
				max_extended_kmer_quality = Math.max(max_extended_kmer_quality, multi);
				logger.trace("multi {}, max_extended_kmer_quality {}", multi, max_extended_kmer_quality);

				if (extend_amount == 1) {
					// running extend_out_left is not needed any more
					extensionSuccess = true;
				} else if (!extensionSuccess) {
					// trace this kmer recursively
					extensionSuccess = extendOutLeft(kmer, 1, extend_amount, kmerRC, kmerAux);
				}
			}
		}

		if (extensionSuccess == true) {
			candidatePath.incKmersQuality(max_extended_kmer_quality * EXTENSION_KMERS_WEIGHT);
			candidatePath.incCoveringKmersWeight(EXTENSION_KMERS_WEIGHT);
			candidatePaths.add(candidatePath);
		}
	}

	//----------------------------------------------------------------------
	// Extends read towards 5'.
	//----------------------------------------------------------------------
	private boolean extendOutLeft(Kmer kmer, int num_extend, int extend_amount, Kmer kmerRC, Kmer kmerAux) {
		int it_alter, multi;
		byte kmerLength = getKmerLength();

		logger.trace("extendOutLeft");

		// generate an initial k-mer
		kmerAux.set(kmer);
		kmerAux.backwardBase((byte)'N', kmerLength);

		// for each nucleotide
		for (it_alter = Kmer.ABASE; it_alter<Kmer.NUM_NUCLEOTIDES; it_alter++) {
			// make a change
			kmerAux.setBase((byte) it_alter, 0);
			//logger.trace("kmer {}", kmerAux.basesToString());
			multi = getKmerMultiplicity(kmerAux, kmerRC);

			if (multi != 0) {
				// k-mer is solid
				logger.trace("multi {}", multi);

				if ((num_extend + 1) == extend_amount) {
					// running extend_out_left is not needed any more
					return true;
				} else {
					// trace  this kmer recursively
					if (extendOutLeft(kmerAux, num_extend + 1, extend_amount,
							kmerRC, KmerGenerator.createKmer())) {
						return true;
					}
				}
			}
		}
		return false;
	}

	//----------------------------------------------------------------------
	// Checks if the read can be extended towards 3'.
	//----------------------------------------------------------------------
	private void performExtendOutRight(Kmer kmer, Sequence sequence, byte[] sequence_tmp, CandidatePath candidatePath, List<CandidatePath> candidatePaths, Kmer kmerRC, Kmer kmerAux) {
		int index_largest_modified, extend_amount, it_alter;
		byte kmerLength = getKmerLength();
		int seqLength = sequence_tmp.length;
		int size = seqLength - kmerLength;
		List<Correction> modifiedBases = candidatePath.getModifiedBases();
		boolean extensionSuccess = false;
		int multi, max_extended_kmer_quality = 0;

		index_largest_modified = seqLength - 1;

		if (modifiedBases.size() > 0) {
			index_largest_modified = modifiedBases.get(modifiedBases.size() - 1).getIndex();
		}

		logger.trace("performExtendOutRight: index_largest_modified {}", index_largest_modified);

		if (index_largest_modified <= size) {
			candidatePaths.add(candidatePath);
			return;
		}

		// extension is needed

		// generate a temporary sequence
		System.arraycopy(sequence.getBases(), 0, sequence_tmp, 0, sequence_tmp.length);

		// applied the modified bases to sequence_tmp
		for(Correction corr: modifiedBases)
			// modify sequence_tmp
			sequence_tmp[corr.getIndex()] = corr.getBase();

		//logger.trace("sequence_tmp {}", new String(sequence_tmp, StandardCharsets.US_ASCII));

		if (index_largest_modified <=  seqLength + MAX_EXTEND - kmerLength) {
			extend_amount = kmerLength - (seqLength - index_largest_modified);
		} else {
			extend_amount = MAX_EXTEND;
		}

		logger.trace("extend_amount {}", extend_amount);

		// generate an initial k-mer
		kmer.set(sequence_tmp, size, kmerLength);
		kmer.forwardBase((byte)'N', kmerLength);

		// for each nucleotide
		for (it_alter = Kmer.ABASE; it_alter<Kmer.NUM_NUCLEOTIDES; it_alter++) {
			// make a change
			kmer.setBase((byte) it_alter, kmerLength - 1);
			//logger.trace("kmer {}", kmer.basesToString(kmerLength));
			multi = getKmerMultiplicity(kmer, kmerRC);

			if (multi != 0) {
				// k-mer is solid
				max_extended_kmer_quality = Math.max(max_extended_kmer_quality, multi);
				logger.trace("multi {}, max_extended_kmer_quality {}", multi, max_extended_kmer_quality);

				if (extend_amount == 1) {
					// running extend_out_right is not needed any more
					extensionSuccess = true;
				} else if (!extensionSuccess) {
					// trace this kmer recursively
					extensionSuccess = extendOutRight(kmer, 1, extend_amount, kmerRC, kmerAux);
				}
			}
		}

		if (extensionSuccess == true) {
			candidatePath.incKmersQuality(max_extended_kmer_quality * EXTENSION_KMERS_WEIGHT);
			candidatePath.incCoveringKmersWeight(EXTENSION_KMERS_WEIGHT);
			candidatePaths.add(candidatePath);
		}
	}

	//----------------------------------------------------------------------
	// Extends read towards 3'.
	//----------------------------------------------------------------------
	private boolean extendOutRight(Kmer kmer, int num_extend, int extend_amount, Kmer kmerRC, Kmer kmerAux) {
		int it_alter, multi;
		byte kmerLength = getKmerLength();

		logger.trace("extendOutRight");

		// generate an initial k-mer
		kmerAux.set(kmer);
		kmerAux.forwardBase((byte)'N', kmerLength);

		// for each nucleotide
		for (it_alter = Kmer.ABASE; it_alter<Kmer.NUM_NUCLEOTIDES; it_alter++) {
			// make a change
			kmerAux.setBase((byte) it_alter, kmerLength - 1);
			//logger.trace("kmer {}", kmerAux.basesToString(kmerLength));
			multi = getKmerMultiplicity(kmerAux, kmerRC);

			if (multi != 0) {
				// k-mer is solid
				logger.trace("multi {}", multi);

				if ((num_extend + 1) == extend_amount) {
					// running extend_out_right is not needed any more
					return true;
				} else {
					// trace  this kmer recursively
					if (extendOutRight(kmerAux, num_extend + 1, extend_amount,
							kmerRC, KmerGenerator.createKmer())) {
						return true;
					}
				}
			}
		}
		return false;
	}

	//----------------------------------------------------------------------
	// Checks if modified first k-mer can be extended.
	//----------------------------------------------------------------------
	private boolean solidFirstKmer(Sequence sequence, CandidatePath path, Kmer firstKmer, Kmer kmerRC, Kmer kmerAux) {
		int index_smallest_modified, extend_amount, it_bases, it_alter, multi;
		byte[] sequenceModified = sequence.getBases();
		byte kmerLength = getKmerLength();
		List<Correction> modifiedBases = path.getModifiedBases();
		byte base;
		boolean extensionSuccess = false;

		// index_smallest_modified
		index_smallest_modified = modifiedBases.get(0).getIndex();

		logger.trace("solidFirstKmer: index_smallest_modified {}", index_smallest_modified);

		// applied the modified bases to first_kmer
		firstKmer.set(sequenceModified, 0, kmerLength);
		for (it_bases = 0; it_bases<modifiedBases.size(); it_bases++) {
			base = Kmer.ENCODE[modifiedBases.get(it_bases).getBase()];
			firstKmer.setBase(base, modifiedBases.get(it_bases).getIndex());
		}

		//logger.trace("firstKmer {}", firstKmer.basesToString(kmerLength));

		if (index_smallest_modified >= kmerLength - MAX_EXTEND - 1) {
			extend_amount = kmerLength - index_smallest_modified - 1;
		} else {
			extend_amount = MAX_EXTEND;
		}

		logger.trace("extend_amount {}", extend_amount);

		// generate an initial k-mer
		kmerAux.set(firstKmer);
		kmerAux.backwardBase((byte)'N', kmerLength);

		// for each nucleotide
		for (it_alter = Kmer.ABASE; it_alter<Kmer.NUM_NUCLEOTIDES; it_alter++) {
			// make a change
			kmerAux.setBase((byte) it_alter, 0);
			//logger.trace("kmer {}", kmerAux.basesToString(kmerLength));
			multi = getKmerMultiplicity(kmerAux, kmerRC);

			if (multi != 0) {
				// k-mer is solid
				logger.trace("multi {}", multi);

				if (extend_amount == 1) {
					// running extend_out_left is not needed any more
					extensionSuccess = true;
					break;
				} else if (!extensionSuccess) {
					// trace this kmer recursively
					extensionSuccess = extendOutLeft(kmerAux, 1, extend_amount, 
							kmerRC, KmerGenerator.createKmer());
				}
			}
		}

		logger.trace("extensionSuccess {}", extensionSuccess);

		return extensionSuccess;
	}

	//----------------------------------------------------------------------
	// Proceeds correction from modified first k-mer towards 3' end.
	//----------------------------------------------------------------------
	private CandidatePath extendFirstKmerToRight(Sequence sequence, CandidatePath pathIn, byte[] basesTemp, Kmer kmer, Kmer kmerRC, Kmer kmerAux, List<MutablePair<Byte,Integer>> modificationsSequence) {
		int it_bases, it_alter, multi, index, checked_changes, max_remaining_changes;
		byte[] sequenceModified = sequence.getBases();
		byte kmerLength = getKmerLength();
		int seqLength = sequence.getLength();
		List<Correction> modifiedBases = pathIn.getModifiedBases();
		List<CandidatePath> candidatePaths = new ArrayList<CandidatePath>();
		List<CandidatePath> candidatePathsTemp = new ArrayList<CandidatePath>();
		byte base;

		logger.trace("extendFirstKmerToRight");

		// generate the second k-mer
		kmer.set(sequenceModified, 1, kmerLength);
		for (it_bases = 0; it_bases<modifiedBases.size(); it_bases++) {
			base = Kmer.ENCODE[modifiedBases.get(it_bases).getBase()];
			index = modifiedBases.get(it_bases).getIndex();
			logger.trace("index {}, base {}", index, base);
			if (index > 0)
				kmer.setBase(base, index - 1);
		}

		//logger.trace("secondKmer {}", kmer.basesToString());
		multi = getKmerMultiplicity(kmer, kmerRC);

		if (multi != 0) {
			// second_kmer is solid
			logger.trace("secondKmer is solid, multi {}", multi);

			if ((seqLength - kmerLength) == 1) {
				// if this k-mer is the last k-mer in a read
				// running extend_a_kmer_3_prime_end is not needed any more
				pathIn.incKmersQuality(multi);
				pathIn.incCoveringKmersWeight(COVERING_KMERS_WEIGHT);
				candidatePathsTemp.add(pathIn);
			} else if ((seqLength - kmerLength) > 1) {
				base = Kmer.DECODE[kmer.getBase(kmerLength - 1)];

				if (modificationsSequence.size() == 0)
					modificationsSequence.add(new MutablePair<Byte,Integer>(base, multi));
				else
					modificationsSequence.set(0, new MutablePair<Byte,Integer>(base, multi));

				logger.trace("modificationsSequence[0].base {}", base);

				// trace this kmer recursively and update candidate_path_vector_tmp
				checked_changes = 0;
				max_remaining_changes = (int) (MAX_CHANGES_IN_REGION_RATIO * (seqLength - kmerLength));

				extendKmer3PrimeEnd(kmer, 1, sequence, kmerRC, kmerAux, pathIn,
						candidatePathsTemp, max_remaining_changes, 1,
						checked_changes, modificationsSequence);
			}
		} else {
			// second_kmer is not solid
			// for each nucleotide
			for (it_alter = Kmer.ABASE; it_alter<Kmer.NUM_NUCLEOTIDES; it_alter++) {
				base = Kmer.DECODE[it_alter];

				// not equal to the original character
				if (sequenceModified[kmerLength] != base) {
					// make a change
					kmer.setBase((byte) it_alter, kmerLength - 1);
					//logger.trace("kmer {}", kmerAux.basesToString());
					multi = getKmerMultiplicity(kmer, kmerRC);

					if (multi != 0) {
						// k-mer is solid
						logger.trace("multi {}", multi);

						if ((seqLength - kmerLength) == 1) {
							// if this k-mer is the last k-mer in a read
							// running extend_a_kmer_3_prime_end is not needed any more
							CandidatePath path = new CandidatePath(pathIn);
							path.incKmersQuality(multi);
							path.incCoveringKmersWeight(COVERING_KMERS_WEIGHT);
							path.addCorrection(new Correction((short) kmerLength, base));
							candidatePathsTemp.add(path);
						} else if ((seqLength - kmerLength) > 1) {
							if (modificationsSequence.size() == 0)
								modificationsSequence.add(new MutablePair<Byte,Integer>(base, multi));
							else
								modificationsSequence.set(0, new MutablePair<Byte,Integer>(base, multi));

							logger.trace("modificationsSequence[0].base {}", base);

							// trace this kmer recursively and update candidate_path_vector_tmp
							checked_changes = 0;
							max_remaining_changes = (int) (MAX_CHANGES_IN_REGION_RATIO * (seqLength - kmerLength)) - 1;

							extendKmer3PrimeEnd(kmer, 1, sequence, kmerRC, kmerAux, pathIn,
									candidatePathsTemp, max_remaining_changes, 1,
									checked_changes, modificationsSequence);
						}
					}
				}
			}
		}

		// check the solidness of the rightmost k-mers of each modified base
		// for each candidate path
		for (CandidatePath path: candidatePathsTemp) {
			logger.trace("extending " + path.getKmersQuality() + " " + path.getCoveringKmersWeight() + " " + path.getModifiedBases().size());
			// generate a temporary sequence
			performExtendOutRight(kmer, sequence, basesTemp, path, candidatePaths, kmerRC, kmerAux);
		}

		if (logger.isTraceEnabled()) {
			logger.trace("candidatePaths size {}", candidatePaths.size());
			for (CandidatePath path: candidatePaths)
				logger.trace(path.getKmersQuality() + " " + path.getCoveringKmersWeight() + " " + path.getModifiedBases().size());
		}

		CandidatePath bestPath = chooseBestCorrection(sequence, candidatePaths);

		if (logger.isTraceEnabled()) {
			if (bestPath != null) {
				logger.trace("correctionSuccess, best path");
				logger.trace(pathIn.getKmersQuality() + " " + pathIn.getCoveringKmersWeight() + " " + pathIn.getModifiedBases().size());
			}
		}

		return bestPath;
	}

	//----------------------------------------------------------------------
	// Applies changes into the read.
	//----------------------------------------------------------------------
	private int modifyErrors(Sequence sequence, List<CandidatePath> candidatePaths, byte[] basesMod) {
		int index, num_corrected_errors = 0;
		byte base;
		byte[] sequenceModified = sequence.getBases();

		CandidatePath bestPath = chooseBestCorrection(sequence, candidatePaths);

		if (bestPath != null) {
			// each modification
			for (Correction corr: bestPath.getModifiedBases()) {
				// update sequence
				index = corr.getIndex();
				base = corr.getBase();
				logger.trace("Correction: index {} base {}", index, base);
				sequenceModified[index] = base;
				if (basesMod != null)
					basesMod[index] = base;
				num_corrected_errors++;
			}

			logger.trace("modifyErrors: {}", num_corrected_errors);
		}

		return num_corrected_errors;
	}

	//----------------------------------------------------------------------
	// Applies changes into the read with respect of modification position.
	//----------------------------------------------------------------------
	private int modifyErrorsFirstKmer(Sequence sequence, byte[] originalBases, List<CandidatePath> candidatePaths, byte[] basesMod) {
		int index, num_corrected_errors1 = 0, num_corrected_errors2 = 0;
		byte base;
		byte[] sequenceModified = sequence.getBases();

		CandidatePath bestPath = chooseBestCorrection(sequence, candidatePaths);

		if (bestPath != null) {
			// each modification
			for (Correction corr: bestPath.getModifiedBases()) {
				// filter out the bases that are equal to the original ones
				index = corr.getIndex();
				base = corr.getBase();

				if (originalBases[index] != base) {
					logger.trace("Correction: index {} base {}", index, base);
					sequenceModified[index] = base;
					if (basesMod != null)
						basesMod[index] = base;

					if (index < getKmerLength())
						num_corrected_errors1++;
					else
						num_corrected_errors2++;
				}
			}

			logger.trace("modifyErrorsFistKmer: {},{}", num_corrected_errors1, num_corrected_errors2);
		}

		return num_corrected_errors1 + num_corrected_errors2;
	}

	//----------------------------------------------------------------------
	// Chooses the best correction path.
	//----------------------------------------------------------------------
	private CandidatePath chooseBestCorrection(Sequence sequence, List<CandidatePath> candidatePaths) {
		logger.trace("chooseBestCorrection: {} paths", candidatePaths.size());

		if (candidatePaths.size() == 0)
			return null;

		if (candidatePaths.size() == 1) {
			// only one path
			// correction succeeds
			return candidatePaths.get(0);
		}

		byte[] bases = sequence.getBases();
		byte[] quals = sequence.getQuals();
		CandidatePath bestPath = null;
		double best_kmer_quality = 0.0;
		double nucleotides_probability = 1.0;
		double kmerQual;
		int it_first_mod, index, it_mod;
		byte base;

		// each candidate path
		for (CandidatePath path: candidatePaths) {
			it_first_mod = 0;
			nucleotides_probability = 1.0;
			// each modification
			for (Correction corr: path.getModifiedBases()) {
				index = corr.getIndex();
				base = corr.getBase();

				if (bases[index] != base) {
					nucleotides_probability = convertQuality2Probability(quals[index]);
					logger.trace("nucleotides_probability(1) {}", nucleotides_probability);
					break;
				}
				it_first_mod++;
			}

			for (it_mod = it_first_mod +1 ; it_mod < path.getModifiedBases().size(); it_mod++) {
				// multiply bases' error probabilities
				index = path.getModifiedBases().get(it_mod).getIndex();
				base = path.getModifiedBases().get(it_mod).getBase();

				if (bases[index] != base) {
					nucleotides_probability *= convertQuality2Probability(quals[index]);
					logger.trace("nucleotides_probability(2) {}", nucleotides_probability);
				}
			}

			kmerQual = path.getKmersQuality() / path.getCoveringKmersWeight();
			kmerQual *= nucleotides_probability;
			path.setKmersQuality(kmerQual);

			if (path.getKmersQuality() > best_kmer_quality) {
				best_kmer_quality = path.getKmersQuality();
				bestPath = path;
			}
		}

		logger.trace("best_kmer_quality {}", best_kmer_quality);

		// correction succeeds
		if (best_kmer_quality > MIN_BEST_KMER_QUALITY)
			return bestPath;

		return null;
	}

	//----------------------------------------------------------------------
	// Converts the quality indicator into an error probability.
	//----------------------------------------------------------------------
	private double convertQuality2Probability(byte qual) {
		qual -= QS_OFFSET;
		return Math.pow(10.0, -qual / 10.0);
	}
}
