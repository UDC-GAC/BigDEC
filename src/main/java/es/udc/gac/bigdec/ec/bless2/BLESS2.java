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
package es.udc.gac.bigdec.ec.bless2;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

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
import es.udc.gac.bigdec.kmer.Kmer;
import es.udc.gac.bigdec.kmer.KmerGenerator;
import es.udc.gac.bigdec.sequence.Sequence;
import es.udc.gac.bigdec.util.Configuration;
import es.udc.gac.bigdec.util.IOUtils;

public class BLESS2 extends CorrectionAlgorithm {

	private static final long serialVersionUID = -2414857720578019984L;
	private static final Logger logger = LoggerFactory.getLogger(BLESS2.class);

	private int MAX_KMER_THRESHOLD;
	private byte QS_OFFSET;
	private byte QS_CUTOFF;
	private byte QS_EXTREMELY_LOW;
	private int MAX_EXTEND;
	private boolean TRIMMING;
	private double MAX_TRIMMING_RATE;
	private double MAX_N_RATIO;
	private double MAX_ERROR_RATIO;
	private byte SUBST_BASE;
	private byte PHRED33;
	private byte PHRED64;
	private double QS_CUTOFF_RATIO;
	private double QS_EXTREMELY_LOW_RATIO;
	private double CHECK_RANGE_RATIO;
	private int MIN_BASES_AFTER_TRIMMING;
	private int MIN_SOLID_LENGTH;
	private int MIN_NON_SOLID_LENGTH;
	private int FP_SUSPECT_LENGTH;
	private int SOLID_REGION_ADJUST_RANGE;
	private int MAX_CANDIDATE_PATHS;
	private int INIT_MIN_QS;
	private int MAX_MODIFICATION;
	private int MIN_QS_DIFF;
	private int NUM_ALLOWABLE_FAILS;
	private int MAX_LOW_QS_BASES;

	public BLESS2(Configuration config, byte kmerLength, int numberOfAlgorithms) {
		super(kmerLength, numberOfAlgorithms);
		MAX_KMER_THRESHOLD = config.BLESS2_MAX_KMER_THRESHOLD;
		MAX_EXTEND = config.BLESS2_MAX_EXTEND;
		TRIMMING = config.BLESS2_TRIMMING;
		MAX_TRIMMING_RATE = config.BLESS2_MAX_TRIMMING_RATE;
		MAX_N_RATIO = config.BLESS2_MAX_N_RATIO;
		MAX_ERROR_RATIO = config.BLESS2_MAX_ERROR_RATIO;
		SUBST_BASE = config.BLESS2_SUBST_BASE.getBytes(StandardCharsets.US_ASCII)[0];
		PHRED33 = config.BLESS2_PHRED33;
		PHRED64 = config.BLESS2_PHRED64;
		QS_CUTOFF_RATIO = config.BLESS2_QS_CUTOFF_RATIO;
		QS_EXTREMELY_LOW_RATIO = config.BLESS2_QS_EXTREMELY_LOW_RATIO;
		CHECK_RANGE_RATIO = config.BLESS2_CHECK_RANGE_RATIO;
		MIN_BASES_AFTER_TRIMMING = config.BLESS2_MIN_BASES_AFTER_TRIMMING;
		MIN_SOLID_LENGTH = config.BLESS2_MIN_SOLID_LENGTH;
		MIN_NON_SOLID_LENGTH = config.BLESS2_MIN_NON_SOLID_LENGTH;
		FP_SUSPECT_LENGTH = config.BLESS2_FP_SUSPECT_LENGTH;
		SOLID_REGION_ADJUST_RANGE = config.BLESS2_SOLID_REGION_ADJUST_RANGE;
		MAX_CANDIDATE_PATHS = config.BLESS2_MAX_CANDIDATE_PATHS;
		INIT_MIN_QS = config.BLESS2_INIT_MIN_QS;
		MAX_MODIFICATION = config.BLESS2_MAX_MODIFICATION;
		MIN_QS_DIFF = config.BLESS2_MIN_QS_DIFF;
		NUM_ALLOWABLE_FAILS = config.BLESS2_NUM_ALLOWABLE_FAILS;
		MAX_LOW_QS_BASES = config.BLESS2_MAX_LOW_QS_BASES;
	}

	@Override
	public String toString() {
		return "BLESS2";
	}

	public void printConfig() {
		IOUtils.info("############# "+this.toString()+" #############");
		IOUtils.info("KMER_LENGTH = " +getKmerLength());
		IOUtils.info("KMER_THRESHOLD = " +getKmerThreshold());
		IOUtils.info("QS_OFFSET = " +QS_OFFSET);
		IOUtils.info("QS_CUTOFF = " +QS_CUTOFF);
		IOUtils.info("MAX_KMER_THRESHOLD = " +MAX_KMER_THRESHOLD);
		IOUtils.info("MAX_EXTEND = " +MAX_EXTEND);
		IOUtils.info("TRIMMING = " +TRIMMING);
		IOUtils.info("MAX_TRIMMING_RATE = " +MAX_TRIMMING_RATE);
		IOUtils.info("MAX_N_RATIO = " +MAX_N_RATIO);
		IOUtils.info("MAX_ERROR_RATIO = " +MAX_ERROR_RATIO);
		IOUtils.info("SUBST_BASE = " +new String(new byte[] {SUBST_BASE}, StandardCharsets.US_ASCII));
		IOUtils.info("PHRED33 = " +PHRED33);
		IOUtils.info("PHRED64 = " +PHRED64);
		IOUtils.info("QS_CUTOFF_RATIO = " +QS_CUTOFF_RATIO);
		IOUtils.info("QS_EXTREMELY_LOW_RATIO = " +QS_EXTREMELY_LOW_RATIO);
		IOUtils.info("CHECK_RANGE_RATIO = " +CHECK_RANGE_RATIO);
		IOUtils.info("MIN_BASES_AFTER_TRIMMING = " +MIN_BASES_AFTER_TRIMMING);
		IOUtils.info("MIN_SOLID_LENGTH = " +MIN_SOLID_LENGTH);
		IOUtils.info("MIN_NON_SOLID_LENGTH = " +MIN_NON_SOLID_LENGTH);
		IOUtils.info("FP_SUSPECT_LENGTH = " +FP_SUSPECT_LENGTH);
		IOUtils.info("SOLID_REGION_ADJUST_RANGE = " +SOLID_REGION_ADJUST_RANGE);
		IOUtils.info("MAX_CANDIDATE_PATHS = " +MAX_CANDIDATE_PATHS);
		IOUtils.info("INIT_MIN_QS = " +INIT_MIN_QS);
		IOUtils.info("MAX_MODIFICATION = " +MAX_MODIFICATION);
		IOUtils.info("MIN_QS_DIFF = " +MIN_QS_DIFF);
		IOUtils.info("NUM_ALLOWABLE_FAILS = " +NUM_ALLOWABLE_FAILS);
		IOUtils.info("MAX_LOW_QS_BASES = " +MAX_LOW_QS_BASES);
		IOUtils.info("##################################");
	}

	public void determineQsOffset(FileSystem fs, Path file) throws CorrectionAlgorithmException {}

	public void determineQsCutoff(int[] qsHistogram) throws CorrectionAlgorithmException {
		long total_bases = 0;
		int partial_sum = 0;
		int max_quality_score = -1000;
		int min_quality_score =  1000;
		boolean set_quality_score_cutoff = false;
		boolean set_extremely_low_quality_score = false;
		QS_EXTREMELY_LOW = 0;

		IOUtils.info(this.toString()+": analyzing quality score histogram");

		/*
		 *  Find max quality score
		 */
		for (int i = 0; i < ErrorCorrection.QS_HISTOGRAM_SIZE; i++) {
			total_bases += qsHistogram[i];
			if (qsHistogram[i] > 0)
				max_quality_score = i;
		}

		/*
		 *  Find min quality score
		 */
		for (int i = ErrorCorrection.QS_HISTOGRAM_SIZE - 1; i >= 0; i--) {
			if (qsHistogram[i] > 0)
				min_quality_score = i;
		}

		/*
		 * Determine the quality score offset
		 */
		if (min_quality_score < 33) {
			IOUtils.error("There is a quality score < 33");
		} else if (min_quality_score <= 58) {
			/*
			 *  33 <= min_quality_score <= 58
			 */
			if (max_quality_score <= 74) {
				QS_OFFSET = PHRED33;
			} else {
				throw new CorrectionAlgorithmException("Irregular quality score range "+min_quality_score+ "-"+max_quality_score);
			}
		} else if (min_quality_score <= 74) {
			/*
			 *  59 <= min_quality_score <= 74
			 */
			if (max_quality_score <= 74) {
				IOUtils.warn("Hard to determine the quality score offset (min: "+min_quality_score+", max: "+max_quality_score+"). Phred+33 will be used");
				QS_OFFSET = PHRED33;
			} else {
				/*
				 * max_quality_score >= 75
				 */
				QS_OFFSET = PHRED64;
			}
		} else {
			/*
			 * min_quality_score >= 75
			 */
			QS_OFFSET = PHRED64;
		}

		/*
		 * Determine the quality score threshold
		 */
		for (int i = 0; i < ErrorCorrection.QS_HISTOGRAM_SIZE; i++) {
			partial_sum += qsHistogram[i];

			if (((1.0 * partial_sum / total_bases) >= QS_CUTOFF_RATIO) && (set_quality_score_cutoff == false)) {
				QS_CUTOFF = (byte)(i - QS_OFFSET);
				set_quality_score_cutoff = true;
			}

			if (((1.0 * partial_sum / total_bases) >= QS_EXTREMELY_LOW_RATIO) && (set_extremely_low_quality_score == false)) {
				QS_EXTREMELY_LOW = (byte) (i - QS_OFFSET);
				set_extremely_low_quality_score = true;

				if ((1.0 * partial_sum / total_bases) >= (QS_EXTREMELY_LOW_RATIO + 0.05)) {
					IOUtils.warn("Some quality score thresholds are set to a high value because overall quality scores are too bad. It may cause long runtime and large memory usage");
				}
			}
		}

		IOUtils.info("quality score offset = " +QS_OFFSET);
		IOUtils.info("quality score threshold = " +QS_CUTOFF);
		IOUtils.info("low quality score threshold = " +QS_EXTREMELY_LOW);
	}

	public void determineKmerCutoff(int[] kmerHistogram) throws CorrectionAlgorithmException {
		int valleyPoint = 0;
		boolean getValleyPoint = false;
		long previousValue = kmerHistogram[ErrorCorrection.KMER_MIN_COUNTER];

		for (int i = ErrorCorrection.KMER_MIN_COUNTER + 1; i < ErrorCorrection.KMER_HISTOGRAM_SIZE - 1; i++) {
			/*
			 * Find the valey point
			 */
			if (kmerHistogram[i] > previousValue) {
				valleyPoint = i - 1;
				getValleyPoint = true;
				break;
			}

			previousValue = kmerHistogram[i];
		}

		if (!getValleyPoint)
			throw new CorrectionAlgorithmException("No valley point exists in the histogram and no k-mer threshold is given. Please, try different k values");

		if (valleyPoint <= MAX_KMER_THRESHOLD) {
			setKmerThreshold(valleyPoint);
		} else {
			setKmerThreshold(MAX_KMER_THRESHOLD);
			IOUtils.warn("The automatically determined k-mer threshold ("+valleyPoint+") is larger than the maximum value ("+MAX_KMER_THRESHOLD+"). The k-mer threshold is set to "+MAX_KMER_THRESHOLD);
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
		List<SolidRegion> solidRegionsOrg = new ArrayList<SolidRegion>();
		List<SolidRegion> solidRegionsTemp = new ArrayList<SolidRegion>();
		List<SolidRegion> solidRegionsOrgTemp = new ArrayList<SolidRegion>();
		SolidRegion newSolidRegion = new SolidRegion();
		Kmer kmer = KmerGenerator.createKmer();
		Kmer kmerRC = KmerGenerator.createKmer();
		Kmer kmerAux = KmerGenerator.createKmer();
		List<CandidatePath> candidatePaths = new ArrayList<CandidatePath>();
		List<CandidatePath> candidatePathsTemp = new ArrayList<CandidatePath>();
		Trimming trim = new Trimming();
		int numCorrectedErrors = 0;
		int numCorrectedErrorsStep1_1 = 0;
		int numCorrectedErrorsStep1_2 = 0;
		int numCorrectedErrorsStep1_3 = 0;
		int numCorrectedErrorsStep2 = 0;
		int minCheckLength = (int) (seqLength * CHECK_RANGE_RATIO);
		int maxAllowedNs = (int) (seqLength * MAX_N_RATIO);
		int maxTrimmedBases = 0;

		System.arraycopy(bases, 0, basesSafe, 0, basesSafe.length);

		// too short read: no trimming
		if (seqLength > MIN_BASES_AFTER_TRIMMING)
			maxTrimmedBases = Math.min((int)(seqLength * MAX_TRIMMING_RATE), seqLength - MIN_BASES_AFTER_TRIMMING);

		if (logger.isDebugEnabled())
			logger.debug("Correcting {}", sequence.basesToString());

		/*String seq = "CAGACGGAGGTTGGGGTGGGGGGGGGGGTAGTCTTGGTTGGTGGGCACNGTGTGGGNGNCNGNNNTTTGGGGGTG";
		if (!sequence.basesToString().equals(seq))
			return sequence;
		 */

		// Replace N bases
		numOfNs = CorrectionAlgorithm.replaceNs(sequence, SUBST_BASE);		
		tooManyNs = numOfNs > maxAllowedNs;

		logger.debug("minCheckLength {}, numberOfNs {}, maxAllowedNs {}, tooManyNs {}, maxTrimmedBases {}", minCheckLength, numOfNs, maxAllowedNs, tooManyNs, maxTrimmedBases);

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
				logger.trace("solid {}", multi);
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
					solidRegionsOrg.add(new SolidRegion(newSolidRegion));
					isSolidKmerPrev = false;
				}
			}
		}

		// last solid region
		if (isSolidKmerPrev == true) {
			newSolidRegion.second(numKmers - 1);
			solidRegions.add(new SolidRegion(newSolidRegion));
			solidRegionsOrg.add(new SolidRegion(newSolidRegion));
		}

		//--------------------------------------------------
		// STEP 0-1: adjust solid regions using quality scores
		//--------------------------------------------------
		// solidRegionsOrg: indexes that are not modified
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

					logger.debug("two solid regions, flagShortDistance {}", flagShortDistance);

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
											solidRegionsOrgTemp.add(solidRegionsOrg.get(it_sr));
										}
									} else {
										// not first low quality base
										if ((it_base - indexPrevLowQualityBase) > kmerLength) {
											solidRegionsTemp.add(new SolidRegion(indexPrevLowQualityBase + 1, it_base - kmerLength));
											solidRegionsOrgTemp.add(solidRegionsOrg.get(it_sr));
										}
									}
									indexPrevLowQualityBase = it_base;
								}
							}

							logger.debug("indexPrevLowQualityBase {}, numLowQualityBase {}", indexPrevLowQualityBase, numLowQualityBase);

							// process the bases to the right of the rightmost low quality base
							if (numLowQualityBase > 0) {
								second = solidRegions.get(it_sr).second();
								if (second >= (indexPrevLowQualityBase + kmerLength)) {
									solidRegionsTemp.add(new SolidRegion(indexPrevLowQualityBase + kmerLength, second));
									solidRegionsOrgTemp.add(solidRegionsOrg.get(it_sr));
								}
							} else {
								// no low quality base
								// add the current solid island
								solidRegionsTemp.add(solidRegions.get(it_sr));
								solidRegionsOrgTemp.add(solidRegionsOrg.get(it_sr));
							}
						}

						solidRegions.clear();
						solidRegions.addAll(solidRegionsTemp);
						solidRegionsOrg.clear();
						solidRegionsOrg.addAll(solidRegionsOrgTemp);
					}
				} else if (solidRegions.size() == 1) {
					// only one solid k-mer island
					numLowQualityBase = 0;
					prevLowQualityIndex = 0;

					logger.debug("one solid region");

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
									solidRegionsOrgTemp.add(solidRegionsOrg.get(0));
								}

								prevLowQualityIndex = it_base;
							} else {
								// not first low quality base
								if ((it_base - prevLowQualityIndex) >= (kmerLength + MIN_SOLID_LENGTH)) {
									solidRegionsTemp.add(new SolidRegion(prevLowQualityIndex + 1, it_base - kmerLength));
									solidRegionsOrgTemp.add(solidRegionsOrg.get(0));
								}

								prevLowQualityIndex = it_base;
							}
						}
					}

					logger.debug("prevLowQualityIndex {}, numLowQualityBase {}", prevLowQualityIndex, numLowQualityBase);

					// the above is done only when this procedure does not remove the only solid island
					if (solidRegionsTemp.size() > 0) {
						solidRegions.clear();
						solidRegions.addAll(solidRegionsTemp);
						solidRegionsOrg.clear();
						solidRegionsOrg.addAll(solidRegionsOrgTemp);
					}
				}
			}
		}

		//--------------------------------------------------
		// STEP 0-2: remove short solid regions
		//--------------------------------------------------
		if (solidRegions.size() > 0) {
			solidRegionsTemp.clear();
			solidRegionsOrgTemp.clear();

			for (it_region = 0; it_region < solidRegions.size(); it_region++) {
				if ((solidRegions.get(it_region).second() - solidRegions.get(it_region).first() + 1) >= MIN_SOLID_LENGTH) {
					solidRegionsTemp.add(solidRegions.get(it_region));
					solidRegionsOrgTemp.add(solidRegionsOrg.get(it_region));
				}
			}
			solidRegions.clear();
			solidRegions.addAll(solidRegionsTemp);
			solidRegionsOrg.clear();
			solidRegionsOrg.addAll(solidRegionsOrgTemp);
		}

		//--------------------------------------------------
		// STEP 0-3: remove short non-solid regions
		//--------------------------------------------------
		if (solidRegions.size() > 0) {
			solidRegionsTemp.clear();
			solidRegionsOrgTemp.clear();
			solidRegionsTemp.add(solidRegions.get(0));
			solidRegionsOrgTemp.add(solidRegionsOrg.get(0));

			if (solidRegions.size() > 1) {
				for (it_region = 1; it_region < solidRegions.size(); it_region++) {
					if ((solidRegions.get(it_region).first() - solidRegions.get(it_region - 1).second() - 1) < MIN_NON_SOLID_LENGTH) {
						solidRegionsTemp.get(solidRegionsTemp.size() - 1).second(solidRegions.get(it_region).second());
					} else {
						solidRegionsTemp.add(solidRegions.get(it_region));
						solidRegionsOrgTemp.add(solidRegionsOrg.get(it_region));
					}
				}
			}
			solidRegions.clear();
			solidRegions.addAll(solidRegionsTemp);
			solidRegionsOrg.clear();
			solidRegionsOrg.addAll(solidRegionsOrgTemp);
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
		if (solidRegions.size() > 0) {
			// 1 - (n - 1) solid region
			for (it_sr = 0; it_sr < (solidRegions.size() - 1); it_sr++) {
				first = solidRegions.get(it_sr).first();
				second = solidRegions.get(it_sr).second();

				// sufficient solid regions length
				if ((second - first) > SOLID_REGION_ADJUST_RANGE) {
					for (it_adjust = second; it_adjust > (second - SOLID_REGION_ADJUST_RANGE); it_adjust--) {
						// low quality score
						if (((quals[it_adjust + kmerLength - 1] - QS_OFFSET) < QS_CUTOFF) ||
								((quals[it_adjust] - QS_OFFSET) < QS_CUTOFF)) {
							solidRegions.get(it_sr).second(it_adjust - 1);
							break;
						}
					}
				}
			}

			// last solid region
			int index_solid_region = solidRegions.size() - 1;
			// non-solid k-mers exist at the 3-prime end
			first = solidRegions.get(index_solid_region).first();
			second = solidRegions.get(index_solid_region).second();

			if (second < (seqLength - kmerLength)) {
				// sufficient solid regions length
				if ((second - first) > SOLID_REGION_ADJUST_RANGE) {
					for (it_adjust = second; it_adjust > (second - SOLID_REGION_ADJUST_RANGE); it_adjust--) {
						// low quality score
						if ((quals[it_adjust + kmerLength - 1] - QS_OFFSET) < QS_CUTOFF) {
							solidRegions.get(index_solid_region).second(it_adjust - 1);
							break;
						}
					}
				}
			}

			// non-solid k-mers exist at the 5-prime end
			first = solidRegions.get(0).first();
			second = solidRegions.get(0).second();

			if (first > 0) {
				// sufficient solid regions length
				if ((second - first) > SOLID_REGION_ADJUST_RANGE) {
					for (it_adjust = first; it_adjust < (first + SOLID_REGION_ADJUST_RANGE); it_adjust++) {
						// low quality score
						if ((quals[it_adjust + kmerLength - 1] - QS_OFFSET) < QS_CUTOFF) {
							solidRegions.get(0).first(it_adjust + 1);
							break;
						}
					}
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

		if (logger.isDebugEnabled()) {
			logger.debug("{} solid regions after step 0-7 (shortNonSolidRegion {})", solidRegions.size(), shortNonSolidRegion);
			for(SolidRegion reg: solidRegions)
				logger.debug(reg.first() + " " + reg.second() + " " + (reg.second() - reg.first() + 1));
			logger.debug("{} original solid regions after step 0-7", solidRegionsOrg.size());
			for(SolidRegion reg: solidRegionsOrg)
				logger.debug(reg.first() + " " + reg.second() + " " + (reg.second() - reg.first() + 1));
		}

		//--------------------------------------------------
		// correct errors
		//--------------------------------------------------
		byte[] basesTemp = new byte[seqLength];
		byte[] sequenceMod = new byte[seqLength];
		Arrays.fill(sequenceMod, (byte) 48); //fill with zeros

		if ((solidRegions.size() > 0) && (shortNonSolidRegion == false)) {
			//--------------------------------------------------
			// STEP 1-1: Correct errors between solid regions
			//--------------------------------------------------
			if (solidRegions.size() > 1) {
				// for each solid region
				for (it_region = 1; it_region < solidRegions.size(); it_region++) {
					first = solidRegions.get(it_region).first();
					second = solidRegions.get(it_region - 1).second();

					if ((((first - 1) - (second + 1)) + 1) >= kmerLength) {
						// the bases that may be modified: from (solid_regions[it_region - 1].second + kmer_length) to (solid_regions[it_region].first - 1)
						// they should not be in trimmed regions
						if (((second + kmerLength) < (seqLength - trim.get3End())) && (first > trim.get5End())) {
							numCorrectedErrorsStep1_1 = correctErrorsBetweenSolidRegions(
									sequence, solidRegions.get(it_region - 1).first(),
									second + 1, first - 1, 
									solidRegions.get(it_region).second(),
									solidRegionsOrg.get(it_region - 1).second() - 1,
									solidRegionsOrg.get(it_region).first() - 1,
									trim, solidRegions.size(),
									maxTrimmedBases, minCheckLength,
									basesSafe, basesTemp, kmer, kmerRC, kmerAux,
									candidatePaths, candidatePathsTemp, sequenceMod);

							numCorrectedErrors += numCorrectedErrorsStep1_1;
						}
					}
				}
			}

			//--------------------------------------------------
			// STEP 1-2: Correct errors in the 5' end
			//--------------------------------------------------
			// number of solid regions is >= 1
			if (solidRegions.size() >= 1) {
				first = solidRegions.get(0).first();
				int firstOrg = solidRegionsOrg.get(0).first();
				// the first solid region does not start from the 0-th k-mer in a read
				if (first > 0 && first > trim.get5End()) {
					// the bases that may be modified: from 0 to (solid_regions[0].first - 1)
					// they should not be in trimmed regions
					numCorrectedErrorsStep1_2 = correctErrors5PrimeEnd(sequence, first - 1,
							trim, firstOrg - 1,
							maxTrimmedBases, minCheckLength,
							basesSafe, basesTemp, kmer, kmerRC, kmerAux, candidatePaths,
							candidatePathsTemp, sequenceMod);

					numCorrectedErrors += numCorrectedErrorsStep1_2;
				}
			}

			//--------------------------------------------------
			// STEP 1-3: Correct errors in the 3' end
			//--------------------------------------------------
			// number of solid regions is >= 1
			if (solidRegions.size() >= 1) {
				second = solidRegions.get(solidRegions.size() - 1).second();
				int secondOrg = solidRegionsOrg.get(solidRegions.size() - 1).second();
				// the last solid region does not end in the last k-mer in a read
				if (second < (seqLength - kmerLength)) {
					if ((second + kmerLength) < (seqLength - trim.get3End())) {
						// the bases that may be modified: from (solid_regions[solid_regions.size() - 1].second + kmer_length) to (read_length - 1)
						// they should not be in trimmed regions
						numCorrectedErrorsStep1_3 = correctErrors3PrimeEnd(sequence, second + 1,
								trim, secondOrg + 1,
								maxTrimmedBases, minCheckLength,
								basesSafe, basesTemp, kmer, kmerRC, kmerAux, candidatePaths,
								candidatePathsTemp, sequenceMod);

						numCorrectedErrors += numCorrectedErrorsStep1_3;
					}
				}
			}

			//
			// check whether any modification was made
			//
			if ((numCorrectedErrors == 0) && (trim.get5End() == 0) && (trim.get3End() == 0)) {
				// trim as many bases as possible
				trim.set5End(solidRegions.get(0).getLeftKmer());

				for(SolidRegion reg: solidRegions) {
					if ((trim.get5End() + (seqLength - kmerLength - reg.getRightKmer())) <= maxTrimmedBases) {
						trim.set3End(seqLength - kmerLength - reg.getRightKmer());
						break;
					}
				}
			}

			logger.debug("after step 1: numCorrectedErrors {}, trim3End {}, trim5End {}", numCorrectedErrors, trim.get3End(), trim.get5End());
		} else {
			//--------------------------------------------------
			// no solid region or short weak regions
			//--------------------------------------------------

			//--------------------------------------------------
			// STEP 2-1: Correct errors in the first k-mer
			//--------------------------------------------------
			// find potentially wrong bases
			correctErrorsFirstKmer(sequence, kmer, kmerRC, kmerAux, candidatePaths, sequenceMod);

			if (logger.isDebugEnabled()) {
				logger.debug("candidate paths after correctErrorsFirstKmer");
				for (CandidatePath path: candidatePaths)
					logger.debug(path.getSumQs() + " " + path.getModifiedBases().size());
			}

			// filter some candidates by extending the first k-mer to the left
			if (candidatePaths.size() > 0) {
				// each path
				for (CandidatePath path: candidatePaths) {
					if (path.getModifiedBases().size() == 0) {
						// no modified base
						candidatePathsTemp.add(path);
					} else {
						// there exist modified bases
						boolean reallyModified = false;

						for (Correction corr: path.getModifiedBases()) {
							if (corr.getBase() != bases[corr.getIndex()]) {
								reallyModified = true;
								break;
							}
						}

						if (reallyModified) {
							// check the index of the first modified base
							if (path.getModifiedBases().get(0).getIndex() < (kmerLength - 1)) {
								// extension is needed
								if (solidFirstKmer(sequence, path, kmer, kmerRC, kmerAux))
									candidatePathsTemp.add(path);
							} else {
								// extension is not needed
								candidatePathsTemp.add(path);
							}
						}
					}
				}
			}

			if (logger.isDebugEnabled()) {
				logger.debug("candidate paths after step 2-1");
				for (CandidatePath path: candidatePathsTemp)
					logger.debug(path.getSumQs() + " " + path.getModifiedBases().size());
			}

			candidatePaths.clear();

			//--------------------------------------------------
			// STEP 2-2: extend candidate paths to the right
			//--------------------------------------------------
			if (candidatePathsTemp.size() > 0) {
				boolean runExploration = true;

				// each path
				for (CandidatePath path: candidatePathsTemp) {
					if (runExploration)
						// add this path to candidate_path_vector_tmp_tmp if its correction succeeds
						runExploration = extendFirstKmerToRight(sequence, path, candidatePaths,
								basesTemp, kmer, kmerRC, kmerAux, runExploration);
				}

				// complete exploration was not done because there are too many candidate paths
				// remove all the paths in candidate_path_vector_tmp
				if (!runExploration)
					candidatePaths.clear();
			}

			// remain only really modified paths
			candidatePathsTemp.clear();

			// each path
			for (CandidatePath path: candidatePaths) {
				boolean reallyModified = false;

				for (Correction corr: path.getModifiedBases()) {
					if (corr.getBase() != bases[corr.getIndex()]) {
						reallyModified = true;
						break;
					}
				}

				if (reallyModified)
					candidatePathsTemp.add(path);
			}

			if (logger.isDebugEnabled()) {
				logger.debug("candidate paths after step 2-2");
				for (CandidatePath path: candidatePathsTemp)
					logger.debug(path.getSumQs() + " " + path.getModifiedBases().size());
			}

			//--------------------------------------------------
			// STEP 2-3: choose a final one in candidate_path_vector_tmp_tmp if possible
			//--------------------------------------------------
			if (candidatePathsTemp.size() != 0) {
				numCorrectedErrorsStep2 = modifyErrorsFirstKmer(sequence, candidatePathsTemp,
						trim, minCheckLength, maxTrimmedBases, sequenceMod);
			} else {
				// no path
				// there are solid regions and they were ignored because the length among them is too short
				// okay, trust them
				if (solidRegions.size() > 0) {
					// trim as many bases as possible
					trim.set5End(solidRegions.get(0).getLeftKmer());

					for(SolidRegion reg: solidRegions) {
						if ((trim.get5End() + (seqLength - kmerLength - reg.getRightKmer())) <= maxTrimmedBases) {
							trim.set3End(seqLength - kmerLength - reg.getRightKmer());
							break;
						}
					}
				} else if (kmerLength <= maxTrimmedBases) {
					// the first k-mer has at least one error
					trim.set5End(kmerLength);
				}
			}

			numCorrectedErrors += numCorrectedErrorsStep2;

			logger.debug("after step 2: numCorrectedErrors {}, trim3End {}, trim5End {}", numCorrectedErrors, trim.get3End(), trim.get5End());
		}

		double maxErrors = seqLength * MAX_ERROR_RATIO;

		if (numCorrectedErrors > maxErrors)
			tooManyErrors = true;

		if (tooManyErrors) {
			// Revert changes
			sequence.setBases(basesSafe);
			bases = basesSafe;
		}

		if (!TRIMMING) {
			trim.clear();
		} else {
			// adjust the number of trimmed bases
			if ((trim.get5End() + trim.get3End()) > maxTrimmedBases) {
				if (trim.get3End() <= maxTrimmedBases) {
					trim.set5End(0);
				}
				else if (trim.get5End() <= maxTrimmedBases) {
					trim.set3End(0);
				}
				else {
					trim.set5End(0);
					trim.set3End(maxTrimmedBases);
				}
			}
		}

		// make a trimmed read
		if (TRIMMING && ((trim.get5End() + trim.get3End()) > 0)) {
			logger.debug("trimming: trim3End {}, trim5End {}", trim.get3End(), trim.get5End());
			int len = seqLength - trim.get5End() - trim.get3End();
			sequence.setBases(bases, trim.get5End(), len);
			// Base quality scores
			if (sequence.getQuals() != null)
				sequence.setQuals(quals, trim.get5End(), len);
		}

		if (logger.isDebugEnabled()) {
			logger.debug("trim5End {}, trim3End {}, maxErrors {}, numCorrectedErrors {}, tooManyErrors {}", trim.get5End(), trim.get3End(), maxErrors, numCorrectedErrors, tooManyErrors);	
			logger.debug("sequenceOrig {}", new String(basesSafe, StandardCharsets.US_ASCII));
			logger.debug("sequenceMod  {}", sequence.basesToString());
			logger.debug("sequenceMods {}", new String(sequenceMod, StandardCharsets.US_ASCII));
		}

		return sequence;
	}

	//----------------------------------------------------------------------
	// Corrects errors in a region situated between correct regions.
	//----------------------------------------------------------------------
	private int correctErrorsBetweenSolidRegions(Sequence sequence, int left_first, 
			int index_start, int index_end, int right_second, int org_boundary_left, 
			int org_boundary_right, Trimming trim, int numSolidIslands,
			int maxTrimmedBases, int minCheckLength, byte[] basesSafe, byte[] basesTemp,
			Kmer kmer, Kmer kmerRC, Kmer kmerAux, List<CandidatePath> candidatePaths,
			List<CandidatePath> candidatePathsTemp, byte[] sequenceMod) {

		byte[] bases = sequence.getBases();
		byte kmerLength = getKmerLength();
		byte[] quals = sequence.getQuals();
		int seqLength = sequence.getLength();
		byte base;
		int numCorrectedErrors = 0;
		int i, it_alter, it_base, it_mod_base, it_check, multi;
		int index, index_last_modified_base, nMods, num_success, num_fails;
		CandidatePath it_path_1st = null;
		List<Correction> modifiedBases;
		boolean runExploration = true, reallyModified, tooManyCorrections;

		candidatePaths.clear();
		candidatePathsTemp.clear();

		// index of the k-mer that can be modified
		// k-mers that are overlapped with a solid region cannot be modified
		int index_last_mod = index_end - kmerLength + 1;

		logger.debug("correctErrorsBetweenSolidRegions {},{},{}", index_start, index_end, index_last_mod);
		logger.debug("left_first {}, right_second {}, org_boundary_left {}, org_boundary_right {}", left_first, right_second, org_boundary_left, org_boundary_right);

		// make an initial k-mer
		kmer.set(bases, index_start, kmerLength);
		//logger.trace("km {}", kmer.basesToString(kmerLength));

		// for each nucleotide
		for (it_alter = Kmer.ABASE; it_alter<Kmer.NUM_NUCLEOTIDES; it_alter++) {
			// make a change
			base = (byte) it_alter;
			kmer.setBase(base, kmerLength - 1);
			//logger.trace("it_alter {} km {}", it_alter, kmer.basesToString(kmerLength));

			multi = getKmerMultiplicity(kmer, kmerRC);

			if (multi != 0) {
				// k-mer is solid
				logger.trace("multi {}", multi);

				index = index_start + kmerLength - 1;
				base = Kmer.DECODE[base];

				// generate a new path
				CandidatePath candidatePath = new CandidatePath();

				if (bases[index] != base)
					candidatePath.addCorrection(new Correction((short)(index), base));			

				// if this k-mer is the last k-mer that can be modified
				// running extend_a_kmer_right is not needed any more
				if (index_start == index_last_mod) {
					candidatePaths.add(candidatePath);

					if (logger.isDebugEnabled()) {
						logger.debug("candidatePath corrections");
						for (Correction corr: candidatePath.getModifiedBases()) {
							logger.debug("{} {}", corr.getIndex(), corr.getBase());
						}
					}
				} else {
					// trace  this kmer recursively and update candidatePathsTemp
					runExploration = extendKmer(kmer, index_start, index_last_mod, sequence,
							kmerRC, kmerAux, candidatePath, candidatePaths, org_boundary_left,
							org_boundary_right, runExploration);
				}
			}
		}

		if (logger.isDebugEnabled()) {
			logger.debug("candidatePaths size {}, runExploration {}", candidatePaths.size(), runExploration);
			for (CandidatePath path: candidatePaths)
				logger.debug(path.getSumQs() + " " +path.getModifiedBases().size());
		}

		// complete exploration was not done because there are too many candidate paths
		// remove all the paths in candidatePaths
		if (runExploration == false)
			candidatePaths.clear();

		boolean allSolidWoModification = false;

		// check the solidness of k-mers between index_last_mod and index_end
		allSolidWoModification = false;
		List<Correction> modifiedBasesList;

		// for each candidate path
		for (CandidatePath path: candidatePaths) {
			if (path.getModifiedBases().size() == 0) {
				allSolidWoModification = true;
				break;
			}

			modifiedBasesList = path.getModifiedBases();
			nMods = modifiedBasesList.size() - 1;
			index_last_modified_base = modifiedBasesList.get(nMods).getIndex();

			logger.debug("index_last_modified_base, index_last_mod {},{}", index_last_modified_base, index_last_mod);

			if (index_last_modified_base > index_last_mod) {
				// checking is needed
				// generate a temporary sequence
				System.arraycopy(bases, 0, basesTemp, 0, bases.length);

				// applied the modified bases to sequence_tmp
				for(Correction corr: modifiedBasesList)
					basesTemp[corr.getIndex()] = corr.getBase();

				//logger.trace("sequence_tmp {}", new String(basesTemp, StandardCharsets.US_ASCII));

				// check k-mers
				num_success = 0;
				num_fails = 0;

				for (it_check = index_last_mod; it_check <= index_last_modified_base; it_check++) {
					kmer.set(basesTemp, it_check, kmerLength);
					//logger.trace("kmer_current {}", kmer.basesToString(kmerLength));

					multi = getKmerMultiplicity(kmer, kmerRC);

					if (multi != 0) {
						// k-mer is solid
						num_success++;
					} else {
						num_fails++;

						if (num_fails > NUM_ALLOWABLE_FAILS)
							break;
					}
				}

				logger.debug("num_success {}, num_fails {}", num_success, num_fails);

				if (num_success >= (index_last_modified_base - index_last_mod + 1 - NUM_ALLOWABLE_FAILS))
					candidatePathsTemp.add(path);
			} else {
				// checking is not needed
				candidatePathsTemp.add(path);
			}
		}

		if (logger.isDebugEnabled()) {
			logger.debug("candidatePathsTemp size {}, allSolidWoModification {}", candidatePathsTemp.size(), allSolidWoModification);
			for (CandidatePath path: candidatePathsTemp)
				logger.debug(path.getSumQs() + " " + path.getModifiedBases().size());
		}

		// remain only really modified paths
		candidatePaths.clear();

		// for each candidate path
		for (CandidatePath path: candidatePathsTemp) {
			// each modification
			reallyModified = false;

			for (Correction corr: path.getModifiedBases()) {
				if (basesSafe[corr.getIndex()] != corr.getBase())
					reallyModified = true;
			}

			if (reallyModified)
				candidatePaths.add(path);
		}

		if (logger.isDebugEnabled()) {
			logger.debug("final candidatePaths size {}", candidatePaths.size());
			for (CandidatePath path: candidatePaths)
				logger.debug(path.getSumQs() + " " +path.getModifiedBases().size());
		}

		// all k-mers are solid without any modification
		if (allSolidWoModification == true)
			// do nothing
			return 0;

		if (candidatePaths.size() > 1) {
			// compare quality scores of candidate paths
			// if the number of paths in candidate_path_vector is larger than 1
			int qs_1st = INIT_MIN_QS;
			int qs_2nd = INIT_MIN_QS;

			// for each candidate path
			for (CandidatePath path: candidatePaths) {
				// each modification
				for (Correction corr: path.getModifiedBases()) {
					if (bases[corr.getIndex()] != corr.getBase()) {
						// add quality scores of modified bases
						path.incQuality(quals[corr.getIndex()] - QS_OFFSET);
					}
				}

				// compare quality scores of each path
				if (path.getSumQs() <= qs_1st) {
					qs_2nd = qs_1st;
					qs_1st = path.getSumQs();
					it_path_1st = path;
				}
				else if (path.getSumQs() <= qs_2nd) {
					qs_2nd = path.getSumQs();
				}
			}

			if (logger.isDebugEnabled()) {
				logger.debug("candidatePaths size {}", candidatePaths.size());
				for (CandidatePath path: candidatePaths)
					logger.debug(path.getSumQs() + " " +path.getModifiedBases().size());
			}

			// check whether too many bases are modified in the first path, which implies indel may exist in the original read
			// if too many modifications exist, the read is just trimmed
			tooManyCorrections = false;
			modifiedBases = it_path_1st.getModifiedBases();
			int numBases = modifiedBases.size();
			int result;

			// this is done only when the number of solid islands is two
			if (numSolidIslands == 2) {
				logger.trace("two islands");

				if (((index_start - left_first) > MAX_MODIFICATION) &&
						((right_second - index_end + 2) <= MAX_MODIFICATION)) {
					// the 1st island is small && the second island is big

					result = modifiedBases.get(numBases - 1).getIndex() - kmerLength - index_start + 2;

					// at least over MAX_MODIFICATION times of modifications from the first modified position
					if ((Integer.compareUnsigned(result, minCheckLength) >= 0) &&
							(numBases > MAX_MODIFICATION)) {
						// each modified base in a right range
						for (it_mod_base = numBases - 1; it_mod_base >= MAX_MODIFICATION; it_mod_base--) {
							// at least MAX_MODIFICATION times of modifications within min_check_length bases
							if ((modifiedBases.get(it_mod_base).getIndex() - modifiedBases.get(it_mod_base - MAX_MODIFICATION).getIndex()) < minCheckLength) {
								// trim_5_end or trim_3_end
								if ((seqLength - modifiedBases.get(it_mod_base).getIndex()) <= maxTrimmedBases)
									trim.set3End(seqLength - modifiedBases.get(it_mod_base).getIndex());
								else if ((modifiedBases.get(it_mod_base).getIndex() + 1) <= maxTrimmedBases)
									trim.set5End(modifiedBases.get(it_mod_base).getIndex() + 1);

								tooManyCorrections = true;
								break;
							}
						}
					}
				} else if (((index_start - left_first) <= MAX_MODIFICATION) && ((right_second - index_end + 2) > MAX_MODIFICATION)) {
					// the 1st island is big && the second island is small

					result = index_end - modifiedBases.get(0).getIndex() + 1;

					// at least over MAX_MODIFICATION times of modifications from the first modified position
					if ((Integer.compareUnsigned(result, minCheckLength) >= 0) &&
							(numBases > MAX_MODIFICATION)) {
						// each modified base in a right range
						for (it_mod_base = 0; it_mod_base < numBases - MAX_MODIFICATION; it_mod_base++) {
							// at least MAX_MODIFICATION times of modifications within min_check_length bases
							if ((modifiedBases.get(it_mod_base + MAX_MODIFICATION).getIndex() - modifiedBases.get(it_mod_base).getIndex()) < minCheckLength) {
								// trim_5_end or trim_3_end
								if ((seqLength - modifiedBases.get(it_mod_base).getIndex()) <= maxTrimmedBases)
									trim.set3End(seqLength - modifiedBases.get(it_mod_base).getIndex());
								else if ((modifiedBases.get(it_mod_base).getIndex() + 1) <= maxTrimmedBases)
									trim.set5End(modifiedBases.get(it_mod_base).getIndex() + 1);

								tooManyCorrections = true;
								break;
							}
						}
					}
				}
			}

			logger.debug("tooManyCorrections {}, trim3End {}, trim5End {}", tooManyCorrections, trim.get3End(), trim.get5End());

			// use the 1st path if not too many corrections are made AND if the 1st path has a sufficiently low score
			// if an indel exists using quality scores is not a good way to choose the best path
			if ((tooManyCorrections == false) && (qs_1st + MIN_QS_DIFF <= qs_2nd)) {
				// update sequence_modification
				for (Correction corr: modifiedBases) {
					index = corr.getIndex();
					base = corr.getBase();
					sequenceMod[index] = base;
					bases[index] = base;
					logger.debug("Correction(1): index {} base {}", index, base);
					numCorrectedErrors++;
				}
			} else {
				// hard to choose one path
				// A AND B
				List<Correction> baseIntersection = new ArrayList<Correction>();
				// A OR B
				List<Correction> baseUnion = new ArrayList<Correction>();
				// A - B
				List<Correction> baseDifference = new ArrayList<Correction>();

				List<Correction> baseIntersectionPrev = new ArrayList<Correction>(candidatePaths.get(0).getModifiedBases());
				List<Correction> baseUnionPrev = new ArrayList<Correction>(candidatePaths.get(0).getModifiedBases());

				// each candidate path
				for (i = 1; i < candidatePaths.size(); i++) {
					baseIntersection.clear();
					baseUnion.clear();

					baseIntersection(baseIntersectionPrev, candidatePaths.get(i).getModifiedBases(), baseIntersection);
					baseUnion(baseUnionPrev, candidatePaths.get(i).getModifiedBases(), baseUnion);

					baseIntersectionPrev.clear();
					baseIntersectionPrev.addAll(baseIntersection);
					baseUnionPrev.clear();
					baseUnionPrev.addAll(baseUnion);
				}

				baseDifference(baseUnion, baseIntersection, baseDifference);

				if (logger.isDebugEnabled()) {
					logger.debug("baseIntersection size {}", baseIntersection.size());
					for (Correction corr: baseIntersection)
						logger.debug(corr.getIndex() + "," +corr.getBase());
					logger.debug("baseUnion size {}", baseUnion.size());
					for (Correction corr: baseUnion)
						logger.debug(corr.getIndex() + "," +corr.getBase());
					logger.debug("baseDifference size {}", baseDifference.size());
					for (Correction corr: baseDifference)
						logger.debug(corr.getIndex() + "," +corr.getBase());
				}

				// find trimmed region
				// correcting the 5'-end and 3'-end was not done
				// therefore the total number of trimmed bases is 0 yet
				if (baseDifference.size() > 0) {
					boolean keepGoing = true;
					int indexLeftmost = 0;
					int indexRightmost = baseDifference.size() - 1;
					Correction leftmost, rightmost;

					while (keepGoing) {
						leftmost = baseDifference.get(indexLeftmost);
						rightmost = baseDifference.get(indexRightmost);

						// # of trimmed bases at the 5' end: base_vector_difference[vector_index_leftmost].first + 1
						// # of trimmed bases at the 3' end: read_length - base_vector_difference[vector_index_rightmost].first
						if ((leftmost.getIndex() + 1) < (seqLength - rightmost.getIndex())) {
							// the 5'-end is smaller
							// check the total number of trimmed bases
							if ((leftmost.getIndex() + 1 + trim.get3End()) <= maxTrimmedBases) {
								if ((leftmost.getIndex() + 1) > trim.get5End()) {
									trim.set5End(leftmost.getIndex() + 1);
								}

								// two points are met
								if (indexLeftmost == indexRightmost) {
									keepGoing = false;
								} else {
									indexLeftmost++;
								}
							} else {
								// no need for more check
								keepGoing = false;
							}
						} else {
							// the 3'-end is smaller
							// check the total number of trimmed bases
							if ((seqLength - rightmost.getIndex()) <= maxTrimmedBases) {
								if ((seqLength - rightmost.getIndex()) > trim.get3End()) {
									trim.set3End(seqLength - rightmost.getIndex());
								}

								// two points are met
								if (indexLeftmost == indexRightmost) {
									keepGoing = false;
								} else {
									indexRightmost--;
								}
							} else {
								// no need for more check
								keepGoing = false;
							}
						}
					}

					logger.debug("trim3End {}, trim5End {}", trim.get3End(), trim.get5End());

					// find consensus modifications
					for (Correction corr: baseIntersection) {
						// check whether the base is not in the trimmed regions
						if ((corr.getIndex() <  (seqLength - trim.get3End())) &&
								(corr.getIndex() >= trim.get5End())) {
							index = corr.getIndex();
							base = corr.getBase();

							// filter out the bases that are equal to the original ones
							if (bases[index] != base) {
								sequenceMod[index] = base;
								bases[index] = base;
								logger.debug("Correction(2): index {} base {}", index, base);
								numCorrectedErrors++;
							}
						}
					}
				}
			}
		} else if (candidatePaths.size() == 1) {
			// only one path
			// check whether too many bases are modified in the first path, which implies indel may exist in the original read
			// if too many modifications exist, the read is just trimmed
			tooManyCorrections = false;

			// at least over MAX_MODIFICATION times of modifications from the first modified position
			modifiedBases = candidatePaths.get(0).getModifiedBases();
			int numBases = modifiedBases.size();
			int result;

			// this is done only when the number of solid islands is two
			if (numSolidIslands == 2) {
				logger.trace("two islands");

				if (((index_start - left_first) > MAX_MODIFICATION) &&
						((right_second - index_end + 2) <= MAX_MODIFICATION)) {
					// the 1st island is small && the second island is big

					result = modifiedBases.get(numBases - 1).getIndex() - kmerLength - index_start + 2;

					// at least over MAX_MODIFICATION times of modifications from the first modified position
					if ((Integer.compareUnsigned(result, minCheckLength) >= 0) &&
							(numBases > MAX_MODIFICATION)) {
						// each modified base in a right range
						for (it_mod_base = numBases - 1; it_mod_base >= MAX_MODIFICATION; it_mod_base--) {
							// at least MAX_MODIFICATION times of modifications within min_check_length bases
							if ((modifiedBases.get(it_mod_base).getIndex() - modifiedBases.get(it_mod_base - MAX_MODIFICATION).getIndex()) < minCheckLength) {
								// trim_5_end or trim_3_end
								if ((seqLength - modifiedBases.get(it_mod_base).getIndex()) <= maxTrimmedBases) {
									trim.set3End(seqLength - modifiedBases.get(it_mod_base).getIndex());

									// update sequence_modification for the non-trimmed corrections
									if (it_mod_base > 0) {
										for (it_base = 0; it_base < (it_mod_base - 1); it_base++) {
											index = modifiedBases.get(it_base).getIndex();
											base = modifiedBases.get(it_base).getBase();
											sequenceMod[index] = base;
											bases[index] = base;
											logger.debug("Correction(3): index {} base {}", index, base);
											numCorrectedErrors++;
										}
									}
								} else if ((modifiedBases.get(it_mod_base).getIndex() + 1) <= maxTrimmedBases) {
									trim.set5End(modifiedBases.get(it_mod_base).getIndex() + 1);

									// update sequence_modification for the non-trimmed corrections
									if (it_mod_base < (numBases - 1)) {
										for (it_base = (it_mod_base + 1); it_base < numBases; it_base++) {
											index = modifiedBases.get(it_base).getIndex();
											base = modifiedBases.get(it_base).getBase();
											sequenceMod[index] = base;
											bases[index] = base;
											logger.debug("Correction(4): index {} base {}", index, base);
											numCorrectedErrors++;
										}
									}
								}

								tooManyCorrections = true;
								break;
							}
						}
					}

					logger.debug("tooManyCorrections {}, trim3End {}, trim5End {}", tooManyCorrections, trim.get3End(), trim.get5End());

				} else if (((index_start - left_first) <= MAX_MODIFICATION) && ((right_second - index_end + 2) > MAX_MODIFICATION)) {
					// the 1st island is big && the second island is small

					result = index_end - modifiedBases.get(0).getIndex() + 1;

					// at least over MAX_MODIFICATION times of modifications from the first modified position
					if ((Integer.compareUnsigned(result, minCheckLength) >= 0) &&
							(numBases > MAX_MODIFICATION)) {
						// each modified base in a right range
						for (it_mod_base = 0; it_mod_base < numBases - MAX_MODIFICATION; it_mod_base++) {
							// at least MAX_MODIFICATION times of modifications within min_check_length bases
							if ((modifiedBases.get(it_mod_base + MAX_MODIFICATION).getIndex() - modifiedBases.get(it_mod_base).getIndex()) < minCheckLength) {
								// trim_5_end or trim_3_end
								if ((seqLength - modifiedBases.get(it_mod_base).getIndex()) <= maxTrimmedBases)
									trim.set3End(seqLength - modifiedBases.get(it_mod_base).getIndex());
								else if ((modifiedBases.get(it_mod_base).getIndex() + 1) <= maxTrimmedBases)
									trim.set5End(modifiedBases.get(it_mod_base).getIndex() + 1);

								tooManyCorrections = true;
								break;
							}
						}
					}

					logger.debug("tooManyCorrections {}, trim3End {}, trim5End {}", tooManyCorrections, trim.get3End(), trim.get5End());
				}
			}

			if (tooManyCorrections == false) {
				// not too many modified bases
				// update sequence_modification
				for (Correction corr: candidatePaths.get(0).getModifiedBases()) {
					index = corr.getIndex();
					base = corr.getBase();
					sequenceMod[index] = base;
					bases[index] = base;
					logger.debug("Correction(5): index {} base {}", index, base);
					numCorrectedErrors++;
				}
			}
		} else if (candidatePaths.size() == 0) {
			// no path
			// if indels exist between two solid k-mer islands, checking the solidness of k-mers between index_last_mod and index_end will always fails
			// this kind of errors cannot be corrected without modifying existing solid k-mer islands
			// one of the both the sides should be trimmed
			if ((seqLength - (index_start + kmerLength - 1)) <= maxTrimmedBases) {
				// trim all the bases to the right of the 1st solid k-mer island
				trim.set3End(seqLength - (index_start + kmerLength - 1));
			} else if ((index_end + 1) <= maxTrimmedBases) {
				// trim all the bases to the left of the 2nd solid k-mer island
				trim.set5End(index_end + 1);
			} else if ((seqLength - (org_boundary_left + kmerLength - 1)) <= maxTrimmedBases) {
				// trim all the bases to the right of the 1st solid k-mer island
				trim.set3End(seqLength - (org_boundary_left + kmerLength - 1));
			} else if ((org_boundary_right + 1) <= maxTrimmedBases) {
				// trim all the bases to the left of the 2nd solid k-mer island
				trim.set5End(org_boundary_right + 1);
			}

			logger.debug("no path, trim3End {}, trim5End {}", trim.get3End(), trim.get5End());
		}

		return numCorrectedErrors;
	}

	//----------------------------------------------------------------------
	// Corrects errors situated on a 5' end of the read
	//----------------------------------------------------------------------
	private int correctErrors5PrimeEnd(Sequence sequence, int index_start, Trimming trim,
			int org_boundary, int maxTrimmedBases, int minCheckLength,
			byte[] basesSafe, byte[] basesTemp, Kmer kmer, Kmer kmerRC, Kmer kmerAux,
			List<CandidatePath> candidatePaths, List<CandidatePath> candidatePathsTemp,
			byte[] sequenceMod) {

		byte[] bases = sequence.getBases();
		byte[] quals = sequence.getQuals();
		byte kmerLength = getKmerLength();
		int seqLength = sequence.getLength();
		int numCorrectedErrors = 0;
		byte base;
		int i, it_alter, it_base, it_mod_base, index, multi;
		CandidatePath candidatePath = null;
		CandidatePath it_path_1st = null;
		List<Correction> modifiedBases;
		boolean runExploration = true, reallyModified, tooManyCorrections;

		logger.debug("correctErrors5PrimeEnd {},{}", index_start, org_boundary);

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

				// generate a new path
				candidatePath = new CandidatePath();

				if (bases[index_start] != base)
					candidatePath.addCorrection(new Correction((short)(index_start), base));

				// if this k-mer is the first k-mer in a read
				// running extend_a_kmer_5_prime_end is not needed any more
				if (index_start == 0) {
					candidatePaths.add(candidatePath);

					if (logger.isDebugEnabled()) {
						logger.debug("candidatePath corrections");
						for (Correction corr: candidatePath.getModifiedBases()) {
							logger.debug("{} {}", corr.getIndex(), corr.getBase());
						}
					}
				} else if (index_start > 0) {
					// trace this kmer recursively and update candidate_path_vector_tmp
					runExploration = extendKmer5PrimeEnd(kmer, index_start, sequence, kmerRC, kmerAux, 
							candidatePath, candidatePaths, org_boundary, runExploration);
				}
			}
		}

		if (logger.isDebugEnabled()) {
			logger.debug("candidatePaths size {}, runExploration {}", candidatePaths.size(), runExploration);
			for (CandidatePath path: candidatePaths)
				logger.debug(path.getSumQs() + " " +path.getModifiedBases().size());
		}

		// complete exploration was not done because there are too many candidate paths
		// remove all the paths in candidatePaths
		if (runExploration == false)
			candidatePaths.clear();

		// check the solidness of the rightmost k-mers of each modified base
		boolean allSolidWoModification = false;

		// for each candidate path
		for (CandidatePath path: candidatePaths) {
			if (path.getModifiedBases().size() == 0) {
				allSolidWoModification = true;
				break;
			}

			performExtendOutLeft(kmer, sequence, basesTemp, path, candidatePathsTemp, kmerRC, kmerAux);
		}

		if (logger.isDebugEnabled()) {
			logger.debug("candidatePathsTemp size {}, allSolidWoModification {}", candidatePathsTemp.size(), allSolidWoModification);
			for (CandidatePath path: candidatePathsTemp)
				logger.debug(path.getSumQs() + " " +path.getModifiedBases().size());
		}

		// remain only really modified paths
		candidatePaths.clear();

		// for each candidate path
		for (CandidatePath path: candidatePathsTemp) {
			// each modification
			reallyModified = false;

			for (Correction corr: path.getModifiedBases()) {
				if (basesSafe[corr.getIndex()] != corr.getBase())
					reallyModified = true;
			}

			if (reallyModified)
				candidatePaths.add(path);
		}

		if (logger.isDebugEnabled()) {
			logger.debug("final candidatePaths size {}", candidatePaths.size());
			for (CandidatePath path: candidatePaths) {
				logger.debug(path.getSumQs() + " " +path.getModifiedBases().size());
				for (Correction corr: path.getModifiedBases())
					logger.debug(corr.getIndex() + " " +corr.getBase());
			}
		}

		// all k-mers are solid without any modification
		// do nothing
		if (allSolidWoModification == true)
			return 0;

		if (candidatePaths.size() > 1) {
			// compare quality scores of candidate paths
			// if the number of paths in candidatePaths is larger than 1
			int qs_1st = INIT_MIN_QS;
			int qs_2nd = INIT_MIN_QS;

			// for each candidate path
			for (CandidatePath path: candidatePaths) {
				// each modification
				for (Correction corr: path.getModifiedBases()) {
					if (bases[corr.getIndex()] != corr.getBase()) {
						// add quality scores of modified bases
						path.incQuality(quals[corr.getIndex()] - QS_OFFSET);
					}
				}

				// compare quality scores of each path
				if (path.getSumQs() <= qs_1st) {
					qs_2nd = qs_1st;
					qs_1st = path.getSumQs();
					it_path_1st = path;
				}
				else if (path.getSumQs() <= qs_2nd) {
					qs_2nd = path.getSumQs();
				}
			}

			if (logger.isDebugEnabled()) {
				logger.debug("candidatePaths size {}", candidatePaths.size());
				for (CandidatePath path: candidatePaths)
					logger.debug(path.getSumQs() + " " +path.getModifiedBases().size());
			}

			// check whether too many bases are modified in the first path, which implies indel may exist in the original read
			// if too many modifications exist, the read is just trimmed
			tooManyCorrections = false;
			modifiedBases = it_path_1st.getModifiedBases();
			int numBases = modifiedBases.size();
			int result;

			// at least over MAX_MODIFICATION times of modifications from the first modified position
			if (((modifiedBases.get(numBases - 1).getIndex() + 1) >= minCheckLength) &&
					(numBases > MAX_MODIFICATION)) {
				// each modified base in a right range
				for (it_mod_base = numBases - 1; it_mod_base >= MAX_MODIFICATION; it_mod_base--) {

					result = modifiedBases.get(it_mod_base).getIndex() - modifiedBases.get(it_mod_base - MAX_MODIFICATION).getIndex(); 

					// at least MAX_MODIFICATION times of modifications within min_check_length bases
					if (Integer.compareUnsigned(result, minCheckLength) < 0) {
						// trim_5_end or trim_3_end
						if ((seqLength - modifiedBases.get(it_mod_base).getIndex()) <= maxTrimmedBases)
							trim.set3End(seqLength - modifiedBases.get(it_mod_base).getIndex());
						else if ((modifiedBases.get(it_mod_base).getIndex() + 1) <= maxTrimmedBases)
							trim.set5End(modifiedBases.get(it_mod_base).getIndex() + 1);

						tooManyCorrections = true;
						break;
					}
				}
			}

			logger.debug("tooManyCorrections {}, trim3End {}, trim5End {}", tooManyCorrections, trim.get3End(), trim.get5End());

			// use the 1st path if not too many corrections are made AND if the 1st path has a sufficiently low score
			// if an indel exists using quality scores is not a good way to choose the best path
			if ((tooManyCorrections == false) && (qs_1st + MIN_QS_DIFF <= qs_2nd)) {
				// update sequence_modification
				for (Correction corr: modifiedBases) {
					index = corr.getIndex();
					base = corr.getBase();
					sequenceMod[index] = base;
					bases[index] = base;
					logger.debug("Correction(1): index {} base {}", index, base);
					numCorrectedErrors++;
				}
			} else {
				// hard to choose one path
				// A AND B
				List<Correction> baseIntersection = new ArrayList<Correction>();
				// A OR B
				List<Correction> baseUnion = new ArrayList<Correction>();
				// A - B
				List<Correction> baseDifference = new ArrayList<Correction>();

				List<Correction> baseIntersectionPrev = new ArrayList<Correction>(candidatePaths.get(0).getModifiedBases());
				List<Correction> baseUnionPrev = new ArrayList<Correction>(candidatePaths.get(0).getModifiedBases());

				// each candidate path
				for (i = 1; i < candidatePaths.size(); i++) {
					baseIntersection.clear();
					baseUnion.clear();

					baseIntersection(baseIntersectionPrev, candidatePaths.get(i).getModifiedBases(), baseIntersection);
					baseUnion(baseUnionPrev, candidatePaths.get(i).getModifiedBases(), baseUnion);

					baseIntersectionPrev.clear();
					baseIntersectionPrev.addAll(baseIntersection);
					baseUnionPrev.clear();
					baseUnionPrev.addAll(baseUnion);
				}

				baseDifference(baseUnion, baseIntersection, baseDifference);

				if (logger.isDebugEnabled()) {
					logger.debug("baseIntersection size {}", baseIntersection.size());
					for (Correction corr: baseIntersection)
						logger.debug(corr.getIndex() + "," +corr.getBase());
					logger.debug("baseUnion size {}", baseUnion.size());
					for (Correction corr: baseUnion)
						logger.debug(corr.getIndex() + "," +corr.getBase());
					logger.debug("baseDifference size {}", baseDifference.size());
					for (Correction corr: baseDifference)
						logger.debug(corr.getIndex() + "," +corr.getBase());
				}

				// find trimmed region
				// correcting the 5'-end and 3'-end was not done
				// therefore the total number of trimmed bases is 0 yet
				if (baseDifference.size() > 0) {
					boolean keepGoing = true;
					int indexLeftmost = 0;
					int indexRightmost = baseDifference.size() - 1;
					Correction leftmost, rightmost;

					while (keepGoing) {
						leftmost = baseDifference.get(indexLeftmost);
						rightmost = baseDifference.get(indexRightmost);

						// # of trimmed bases at the 5' end: base_vector_difference[vector_index_leftmost].first + 1
						// # of trimmed bases at the 3' end: read_length - base_vector_difference[vector_index_rightmost].first
						if ((leftmost.getIndex() + 1) < (seqLength - rightmost.getIndex())) {
							// the 5'-end is smaller
							// check the total number of trimmed bases
							if ((leftmost.getIndex() + 1 + trim.get3End()) <= maxTrimmedBases) {
								if ((leftmost.getIndex() + 1) > trim.get5End()) {
									trim.set5End(leftmost.getIndex() + 1);
								}

								// two points are met
								if (indexLeftmost == indexRightmost) {
									keepGoing = false;
								} else {
									indexLeftmost++;
								}
							} else {
								// no need for more check
								keepGoing = false;
							}
						} else {
							// the 3'-end is smaller
							// check the total number of trimmed bases
							if ((seqLength - rightmost.getIndex()) <= maxTrimmedBases) {
								if ((seqLength - rightmost.getIndex()) > trim.get3End()) {
									trim.set3End(seqLength - rightmost.getIndex());
								}

								// two points are met
								if (indexLeftmost == indexRightmost) {
									keepGoing = false;
								} else {
									indexRightmost--;
								}
							} else {
								// no need for more check
								keepGoing = false;
							}
						}
					}
				}

				logger.debug("trim3End {}, trim5End {}", trim.get3End(), trim.get5End());

				// find consensus modifications
				for (Correction corr: baseIntersection) {
					// check whether the base is not in the trimmed regions
					if ((corr.getIndex() <  (seqLength - trim.get3End())) &&
							(corr.getIndex() >= trim.get5End())) {
						index = corr.getIndex();
						base = corr.getBase();

						// filter out the bases that are equal to the original ones
						if (bases[index] != base) {
							sequenceMod[index] = base;
							bases[index] = base;
							logger.debug("Correction(2): index {} base {}", index, base);
							numCorrectedErrors++;
						}
					}
				}
			}
		} else if (candidatePaths.size() == 1) {
			// only one path
			// check whether too many bases are modified in the first path, which implies indel may exist in the original read
			// if too many modifications exist, the read is just trimmed
			tooManyCorrections = false;

			// at least over MAX_MODIFICATION times of modifications from the first modified position
			modifiedBases = candidatePaths.get(0).getModifiedBases();
			int numBases = modifiedBases.size();
			int result;

			if (((modifiedBases.get(0).getIndex() + 1) >= minCheckLength) &&
					(numBases > MAX_MODIFICATION)) {
				// each modified base in a right range
				for (it_mod_base = numBases - 1; it_mod_base >= MAX_MODIFICATION; it_mod_base--) {
					// at least MAX_MODIFICATION times of modifications within min_check_length bases

					result = modifiedBases.get(it_mod_base).getIndex() - modifiedBases.get(it_mod_base - MAX_MODIFICATION).getIndex();

					if (Integer.compareUnsigned(result, minCheckLength) < 0) {
						// trim_5_end or trim_3_end
						if ((seqLength - modifiedBases.get(it_mod_base).getIndex()) <= maxTrimmedBases) {
							trim.set3End(seqLength - modifiedBases.get(it_mod_base).getIndex());

							// update sequence_modification for the non-trimmed corrections
							if (it_mod_base > 0) {
								for (it_base = 0; it_base < (it_mod_base - 1); it_base++) {
									index = modifiedBases.get(it_base).getIndex();
									base = modifiedBases.get(it_base).getBase();
									sequenceMod[index] = base;
									bases[index] = base;
									logger.debug("Correction(3): index {} base {}", index, base);
									numCorrectedErrors++;
								}
							}
						} else if ((modifiedBases.get(it_mod_base).getIndex() + 1) <= maxTrimmedBases) {
							trim.set5End(modifiedBases.get(it_mod_base).getIndex() + 1);

							// update sequence_modification for the non-trimmed corrections
							if (it_mod_base < (numBases - 1)) {
								for (it_base = (it_mod_base + 1); it_base < numBases; it_base++) {
									index = modifiedBases.get(it_base).getIndex();
									base = modifiedBases.get(it_base).getBase();
									sequenceMod[index] = base;
									bases[index] = base;
									logger.debug("Correction(4): index {} base {}", index, base);
									numCorrectedErrors++;
								}
							}
						}

						tooManyCorrections = true;
						break;
					}
				}
			}

			logger.debug("tooManyCorrections {}, trim3End {}, trim5End {}", tooManyCorrections, trim.get3End(), trim.get5End());

			if (tooManyCorrections == false) {
				// not too many modified bases
				// update sequence_modification
				for (Correction corr: candidatePaths.get(0).getModifiedBases()) {
					index = corr.getIndex();
					base = corr.getBase();
					sequenceMod[index] = base;
					bases[index] = base;
					logger.debug("Correction(5): index {} base {}", index, base);
					numCorrectedErrors++;
				}
			}
		}

		return numCorrectedErrors;
	}

	//----------------------------------------------------------------------
	// Corrects errors situated on a 3' end of the read
	//----------------------------------------------------------------------
	private int correctErrors3PrimeEnd(Sequence sequence, int index_start, Trimming trim,
			int org_boundary, int maxTrimmedBases, int minCheckLength,
			byte[] basesSafe, byte[] basesTemp, Kmer kmer, Kmer kmerRC, Kmer kmerAux,
			List<CandidatePath> candidatePaths, List<CandidatePath> candidatePathsTemp,
			byte[] sequenceMod) {

		byte[] bases = sequence.getBases();
		byte[] quals = sequence.getQuals();
		int seqLength = sequence.getLength();
		byte kmerLength = getKmerLength();
		int size = seqLength - kmerLength;
		int numCorrectedErrors = 0;
		byte base;
		int i, it_alter, it_base, it_mod_base, index, multi;
		CandidatePath it_path_1st = null;
		List<Correction> modifiedBases;
		boolean runExploration = true, reallyModified, tooManyCorrections;

		logger.debug("correctErrors3PrimeEnd {},{}", index_start, org_boundary);

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
				index = index_start + kmerLength - 1;

				// generate a new path
				CandidatePath candidatePath = new CandidatePath();

				if (bases[index] != base)
					candidatePath.addCorrection(new Correction((short)(index), base));

				// if this k-mer is the first k-mer in a read
				// running extend_a_kmer_3_prime_end is not needed any more
				if (index_start == size) {
					candidatePaths.add(candidatePath);

					if (logger.isDebugEnabled()) {
						logger.debug("candidatePath corrections");
						for (Correction corr: candidatePath.getModifiedBases()) {
							logger.debug("{} {}", corr.getIndex(), corr.getBase());
						}
					}
				} else if (index_start < size) {
					// trace this kmer recursively and update candidate_path_vector_tmp
					runExploration = extendKmer3PrimeEnd(kmer, index_start, sequence, kmerRC, kmerAux, 
							candidatePath, candidatePaths, org_boundary, runExploration);
				}
			}
		}

		if (logger.isDebugEnabled()) {
			logger.debug("candidatePaths size {}, runExploration {}", candidatePaths.size(), runExploration);
			for (CandidatePath path: candidatePaths)
				logger.debug(path.getSumQs() + " " +path.getModifiedBases().size());
		}

		// complete exploration was not done because there are too many candidate paths
		// remove all the paths in candidatePaths
		if (runExploration == false)
			candidatePaths.clear();

		// check the solidness of the rightmost k-mers of each modified base
		boolean allSolidWoModification = false;

		// for each candidate path
		for (CandidatePath path: candidatePaths) {
			if (path.getModifiedBases().size() == 0) {
				allSolidWoModification = true;
				break;
			}

			performExtendOutRight(kmer, sequence, basesTemp, path, candidatePathsTemp, kmerRC, kmerAux);
		}

		if (logger.isDebugEnabled()) {
			logger.debug("candidatePathsTemp size {}, allSolidWoModification {}", candidatePathsTemp.size(), allSolidWoModification);
			for (CandidatePath path: candidatePathsTemp)
				logger.debug(path.getSumQs() + " " +path.getModifiedBases().size());
		}

		// remain only really modified paths
		candidatePaths.clear();

		// for each candidate path
		for (CandidatePath path: candidatePathsTemp) {
			// each modification
			reallyModified = false;

			for (Correction corr: path.getModifiedBases()) {
				if (basesSafe[corr.getIndex()] != corr.getBase())
					reallyModified = true;
			}

			if (reallyModified)
				candidatePaths.add(path);
		}

		if (logger.isDebugEnabled()) {
			logger.debug("final candidatePaths size {}", candidatePaths.size());
			for (CandidatePath path: candidatePaths)
				logger.debug(path.getSumQs() + " " +path.getModifiedBases().size());
		}

		// all k-mers are solid without any modification
		// do nothing
		if (allSolidWoModification == true)
			return 0;

		if (candidatePaths.size() > 1) {
			// compare quality scores of candidate paths
			// if the number of paths in candidatePaths is larger than 1
			int qs_1st = INIT_MIN_QS;
			int qs_2nd = INIT_MIN_QS;

			// for each candidate path
			for (CandidatePath path: candidatePaths) {
				// each modification
				for (Correction corr: path.getModifiedBases()) {
					if (bases[corr.getIndex()] != corr.getBase()) {
						// add quality scores of modified bases
						path.incQuality(quals[corr.getIndex()] - QS_OFFSET);
					}
				}

				// compare quality scores of each path
				if (path.getSumQs() <= qs_1st) {
					qs_2nd = qs_1st;
					qs_1st = path.getSumQs();
					it_path_1st = path;
				}
				else if (path.getSumQs() <= qs_1st) {
					qs_2nd = path.getSumQs();
				}
			}

			if (logger.isDebugEnabled()) {
				logger.debug("candidatePaths size {}", candidatePaths.size());
				for (CandidatePath path: candidatePaths)
					logger.debug(path.getSumQs() + " " +path.getModifiedBases().size());
			}

			// check whether too many bases are modified in the first path, which implies indel may exist in the original read
			// if too many modifications exist, the read is just trimmed
			tooManyCorrections = false;
			modifiedBases = it_path_1st.getModifiedBases();
			int numBases = modifiedBases.size();
			int result;

			// at least over MAX_MODIFICATION times of modifications from the first modified position
			if (((seqLength - modifiedBases.get(0).getIndex()) >= minCheckLength) &&
					(numBases > MAX_MODIFICATION)) {
				// each modified base in a right range
				for (it_mod_base = 0; it_mod_base < numBases - MAX_MODIFICATION; it_mod_base++) {

					result = modifiedBases.get(it_mod_base + MAX_MODIFICATION).getIndex() - modifiedBases.get(it_mod_base).getIndex();

					// at least MAX_MODIFICATION times of modifications within min_check_length bases
					if (Integer.compareUnsigned(result, minCheckLength) < 0) {
						// trim_5_end or trim_3_end
						if ((seqLength - modifiedBases.get(it_mod_base).getIndex()) <= maxTrimmedBases)
							trim.set3End(seqLength - modifiedBases.get(it_mod_base).getIndex());
						else if ((modifiedBases.get(it_mod_base).getIndex() + 1) <= maxTrimmedBases)
							trim.set5End(modifiedBases.get(it_mod_base).getIndex() + 1);

						tooManyCorrections = true;
						break;
					}
				}
			}

			logger.debug("tooManyCorrections {}, trim3End {}, trim5End {}", tooManyCorrections, trim.get3End(), trim.get5End());

			// use the 1st path if not too many corrections are made AND if the 1st path has a sufficiently low score
			// if an indel exists using quality scores is not a good way to choose the best path
			if ((tooManyCorrections == false) && (qs_1st + MIN_QS_DIFF <= qs_2nd)) {
				// update sequence_modification
				for (Correction corr: modifiedBases) {
					index = corr.getIndex();
					base = corr.getBase();
					sequenceMod[index] = base;
					bases[index] = base;
					logger.debug("Correction(1): index {} base {}", index, base);
					numCorrectedErrors++;
				}
			} else {
				// hard to choose one path
				// A AND B
				List<Correction> baseIntersection = new ArrayList<Correction>();
				// A OR B
				List<Correction> baseUnion = new ArrayList<Correction>();
				// A - B
				List<Correction> baseDifference = new ArrayList<Correction>();

				List<Correction> baseIntersectionPrev = new ArrayList<Correction>(candidatePaths.get(0).getModifiedBases());
				List<Correction> baseUnionPrev = new ArrayList<Correction>(candidatePaths.get(0).getModifiedBases());

				// each candidate path
				for (i = 1; i < candidatePaths.size(); i++) {
					baseIntersection.clear();
					baseUnion.clear();

					baseIntersection(baseIntersectionPrev, candidatePaths.get(i).getModifiedBases(), baseIntersection);
					baseUnion(baseUnionPrev, candidatePaths.get(i).getModifiedBases(), baseUnion);

					baseIntersectionPrev.clear();
					baseIntersectionPrev.addAll(baseIntersection);
					baseUnionPrev.clear();
					baseUnionPrev.addAll(baseUnion);
				}

				baseDifference(baseUnion, baseIntersection, baseDifference);

				if (logger.isDebugEnabled()) {
					logger.debug("baseIntersection size {}", baseIntersection.size());
					for (Correction corr: baseIntersection)
						logger.debug(corr.getIndex() + "," +corr.getBase());
					logger.debug("baseUnion size {}", baseUnion.size());
					for (Correction corr: baseUnion)
						logger.debug(corr.getIndex() + "," +corr.getBase());
					logger.debug("baseDifference size {}", baseDifference.size());
					for (Correction corr: baseDifference)
						logger.debug(corr.getIndex() + "," +corr.getBase());
				}

				// find trimmed region
				// correcting the 5'-end and 3'-end was not done
				// therefore the total number of trimmed bases is 0 yet
				if (baseDifference.size() > 0) {
					boolean keepGoing = true;
					int indexLeftmost = 0;
					int indexRightmost = baseDifference.size() - 1;
					Correction leftmost, rightmost;

					while (keepGoing) {
						leftmost = baseDifference.get(indexLeftmost);
						rightmost = baseDifference.get(indexRightmost);

						// # of trimmed bases at the 5' end: base_vector_difference[vector_index_leftmost].first + 1
						// # of trimmed bases at the 3' end: read_length - base_vector_difference[vector_index_rightmost].first
						if ((leftmost.getIndex() + 1) < (seqLength - rightmost.getIndex())) {
							// the 5'-end is smaller
							// check the total number of trimmed bases
							if ((leftmost.getIndex() + 1 + trim.get3End()) <= maxTrimmedBases) {
								if ((leftmost.getIndex() + 1) > trim.get5End()) {
									trim.set5End(leftmost.getIndex() + 1);
								}

								// two points are met
								if (indexLeftmost == indexRightmost) {
									keepGoing = false;
								} else {
									indexLeftmost++;
								}
							} else {
								// no need for more check
								keepGoing = false;
							}
						} else {
							// the 3'-end is smaller
							// check the total number of trimmed bases
							if ((seqLength - rightmost.getIndex()) <= maxTrimmedBases) {
								if ((seqLength - rightmost.getIndex()) > trim.get3End()) {
									trim.set3End(seqLength - rightmost.getIndex());
								}

								// two points are met
								if (indexLeftmost == indexRightmost) {
									keepGoing = false;
								} else {
									indexRightmost--;
								}
							} else {
								// no need for more check
								keepGoing = false;
							}
						}
					}
				}

				logger.debug("trim3End {}, trim5End {}", trim.get3End(), trim.get5End());

				// find consensus modifications
				for (Correction corr: baseIntersection) {
					// check whether the base is not in the trimmed regions
					if ((corr.getIndex() <  (seqLength - trim.get3End())) &&
							(corr.getIndex() >= trim.get5End())) {
						index = corr.getIndex();
						base = corr.getBase();

						// filter out the bases that are equal to the original ones
						if (bases[index] != base) {
							sequenceMod[index] = base;
							bases[index] = base;
							logger.debug("Correction(2): index {} base {}", index, base);
							numCorrectedErrors++;
						}
					}
				}
			}
		} else if (candidatePaths.size() == 1) {
			// only one path
			// check whether too many bases are modified in the first path, which implies indel may exist in the original read
			// if too many modifications exist, the read is just trimmed
			tooManyCorrections = false;

			// at least over MAX_MODIFICATION times of modifications from the first modified position
			modifiedBases = candidatePaths.get(0).getModifiedBases();
			int numBases = modifiedBases.size();
			int result;

			if (((seqLength - modifiedBases.get(0).getIndex()) >= minCheckLength) &&
					(numBases > MAX_MODIFICATION)) {
				// each modified base in a right range
				for (it_mod_base = 0; it_mod_base < numBases - MAX_MODIFICATION; it_mod_base++) {

					result = modifiedBases.get(it_mod_base + MAX_MODIFICATION).getIndex() - modifiedBases.get(it_mod_base).getIndex();

					// at least MAX_MODIFICATION times of modifications within min_check_length bases					
					if (Integer.compareUnsigned(result, minCheckLength) < 0) {
						// trim_5_end or trim_3_end
						if ((seqLength - modifiedBases.get(it_mod_base).getIndex()) <= maxTrimmedBases) {
							trim.set3End(seqLength - modifiedBases.get(it_mod_base).getIndex());

							// update sequence_modification for the non-trimmed corrections
							if (it_mod_base > 0) {
								for (it_base = 0; it_base < (it_mod_base - 1); it_base++) {
									index = modifiedBases.get(it_base).getIndex();
									base = modifiedBases.get(it_base).getBase();
									sequenceMod[index] = base;
									bases[index] = base;
									logger.debug("Correction(3): index {} base {}", index, base);
									numCorrectedErrors++;
								}
							}
						} else if ((modifiedBases.get(it_mod_base).getIndex() + 1) <= maxTrimmedBases) {
							trim.set5End(modifiedBases.get(it_mod_base).getIndex() + 1);

							// update sequence_modification for the non-trimmed corrections
							if (it_mod_base < (numBases - 1)) {
								for (it_base = (it_mod_base + 1); it_base < numBases; it_base++) {
									index = modifiedBases.get(it_base).getIndex();
									base = modifiedBases.get(it_base).getBase();
									sequenceMod[index] = base;
									bases[index] = base;
									logger.debug("Correction(4): index {} base {}", index, base);
									numCorrectedErrors++;
								}
							}
						}

						tooManyCorrections = true;
						break;
					}
				}
			}

			logger.debug("tooManyCorrections {}, trim3End {}, trim5End {}", tooManyCorrections, trim.get3End(), trim.get5End());

			if (tooManyCorrections == false) {
				// not too many modified bases
				// update sequence_modification
				for (Correction corr: candidatePaths.get(0).getModifiedBases()) {
					index = corr.getIndex();
					base = corr.getBase();
					sequenceMod[index] = base;
					bases[index] = base;
					logger.debug("Correction(5): index {} base {}", index, base);
					numCorrectedErrors++;
				}
			}
		}

		return numCorrectedErrors;
	}

	//----------------------------------------------------------------------
	// Corrects errors in the first k-mer of the read.
	//----------------------------------------------------------------------
	private void correctErrorsFirstKmer(Sequence sequence, Kmer firstKmer, Kmer kmerRC,
			Kmer kmerAux, List<CandidatePath> candidatePaths, byte[] sequenceMod) {
		byte[] sequenceModified = sequence.getBases();
		byte[] quals = sequence.getQuals();
		byte kmerLength = getKmerLength();
		byte base, firstKmerBase;
		int it_alter, it_bases, multi;
		CandidatePath candidatePath = new CandidatePath();
		List<MutablePair<Short,Byte>> lowQsIndexes;

		candidatePaths.clear();

		// make an initial k-mer
		firstKmer.set(sequenceModified, 0, kmerLength);
		//logger.trace("km {}", firstKmer.basesToString(kmerLength));

		lowQsIndexes = new ArrayList<MutablePair<Short,Byte>>(kmerLength);

		for (it_bases = 0; it_bases < kmerLength; it_bases++) {
			if ((quals[it_bases] - QS_OFFSET) < QS_CUTOFF) {
				lowQsIndexes.add(new MutablePair<Short,Byte>((short)it_bases, quals[it_bases]));
			}
		}

		if (logger.isDebugEnabled()) {
			logger.debug("correctErrorsFirstKmer: lowQsIndexes size {}", lowQsIndexes.size());
			for (MutablePair<Short,Byte> qual: lowQsIndexes)
				logger.debug("{}, {}", qual.right, qual.left);
		}

		if (lowQsIndexes.size() == 0) {
			// low quality bases exist
			multi = getKmerMultiplicity(firstKmer, kmerRC);

			if (multi != 0) {
				// the first k-mer is solid
				logger.trace("multi {}", multi);
				candidatePaths.add(new CandidatePath());
			} else {
				// change the bases with the lowest MAX_LOW_QS_BASES quality scores
				List<MutablePair<Short,Byte>> lowQsIndexesSorted = sortIndexes(quals, kmerLength);

				if (logger.isDebugEnabled()) {
					logger.debug("lowQsIndexesSorted size {}", lowQsIndexesSorted.size());
					for (MutablePair<Short,Byte> qual: lowQsIndexesSorted)
						logger.debug("{}, {}", qual.left, qual.right);
				}

				checkFirstKmer(firstKmer, kmerAux, kmerRC, candidatePath, lowQsIndexesSorted,
						candidatePaths, 0, MAX_LOW_QS_BASES - 1);

				// change each base in the first k-mer and check whether it is solid
				// it is still needed because there could be more than MAX_LOW_QS_BASES bases that have
				// lower quality scores than real problematic ones
				for (it_bases = 0; it_bases < kmerLength; it_bases++) {
					kmerAux.set(firstKmer);
					firstKmerBase = firstKmer.getBase(it_bases);

					// each alternative neocletide
					for (it_alter = Kmer.ABASE; it_alter<Kmer.NUM_NUCLEOTIDES; it_alter++) {
						base = (byte) it_alter;

						if (firstKmerBase != base) {
							// not equal to the original character
							// generate a new k-mer
							kmerAux.setBase(base, it_bases);
							//logger.trace("km_tmp {}", kmerAux.basesToString());

							// add kmer_tmp to candidate_path_tmp if it is solid
							multi = getKmerMultiplicity(kmerAux, kmerRC);

							if (multi != 0) {
								// k-mer is solid
								logger.trace("multi {}", multi);
								// generate a new candidate path
								CandidatePath path = new CandidatePath();
								path.addCorrection(new Correction((short)(it_bases), Kmer.DECODE[base]));
								candidatePaths.add(path);
							}
						}
					}
				}
			}
		} else if (lowQsIndexes.size() > 0) {
			// low quality bases exist
			if (lowQsIndexes.size() <= MAX_LOW_QS_BASES) {
				// the number of low-quality bases is smaller than the threshold
				checkFirstKmer(firstKmer, kmerAux, kmerRC, candidatePath, lowQsIndexes,
						candidatePaths, 0, lowQsIndexes.size() - 1);

				// no candidate path is found
				if (candidatePaths.size() == 0) {
					multi = getKmerMultiplicity(firstKmer, kmerRC);

					if (multi != 0) {
						// the first k-mer is solid
						logger.trace("multi {}", multi);
						candidatePaths.add(new CandidatePath());
					} else {
						// change each base in the first k-mer and check whether it is solid
						for (it_bases = 0; it_bases < kmerLength; it_bases++) {
							kmerAux.set(firstKmer);
							firstKmerBase = firstKmer.getBase(it_bases);

							// each alternative neocletide
							for (it_alter = Kmer.ABASE; it_alter<Kmer.NUM_NUCLEOTIDES; it_alter++) {
								base = (byte) it_alter;

								if (firstKmerBase != base) {
									// not equal to the original character
									// generate a new k-mer
									kmerAux.setBase(base, it_bases);
									//logger.trace("km_tmp {}", kmerAux.basesToString());

									// add kmer_tmp to candidate_path_tmp if it is solid
									multi = getKmerMultiplicity(kmerAux, kmerRC);

									if (multi != 0) {
										// k-mer is solid
										logger.trace("multi {}", multi);
										// generate a new candidate path
										CandidatePath path = new CandidatePath();
										path.addCorrection(new Correction((short)(it_bases), Kmer.DECODE[base]));
										candidatePaths.add(path);
									}
								}
							}
						}
					}
				}
			} else {
				// too many low-quality bases
				// change the bases with the lowest MAX_LOW_QS_BASES quality scores
				List<MutablePair<Short,Byte>> lowQsIndexesSorted = sortIndexes(quals, kmerLength);

				if (logger.isDebugEnabled()) {
					logger.debug("lowQsIndexesSorted size {}", lowQsIndexesSorted.size());
					for (MutablePair<Short,Byte> qual: lowQsIndexesSorted)
						logger.debug("{}, {}", qual.left, qual.right);
				}

				checkFirstKmer(firstKmer, kmerAux, kmerRC, candidatePath, lowQsIndexesSorted,
						candidatePaths, 0, MAX_LOW_QS_BASES - 1);

				// change each base in the first k-mer and check whether it is solid
				for (it_bases = 0; it_bases < kmerLength; it_bases++) {
					kmerAux.set(firstKmer);
					firstKmerBase = firstKmer.getBase(it_bases);

					// each alternative neocletide
					for (it_alter = Kmer.ABASE; it_alter<Kmer.NUM_NUCLEOTIDES; it_alter++) {
						base = (byte) it_alter;

						if (firstKmerBase != base) {
							// not equal to the original character
							// generate a new k-mer
							kmerAux.setBase(base, it_bases);
							//logger.trace("km_tmp {}", kmerAux.basesToString());

							// add kmer_tmp to candidate_path_tmp if it is solid
							multi = getKmerMultiplicity(kmerAux, kmerRC);

							if (multi != 0) {
								// k-mer is solid
								logger.trace("multi {}", multi);
								// generate a new candidate path
								CandidatePath path = new CandidatePath();
								path.addCorrection(new Correction((short)(it_bases), Kmer.DECODE[base]));
								candidatePaths.add(path);
							}
						}
					}
				}

				multi = getKmerMultiplicity(firstKmer, kmerRC);

				if (multi != 0) {
					// the first k-mer is solid
					logger.trace("firstKmer multi {}", multi);
					candidatePaths.add(new CandidatePath());
				}
			}
		}
	}

	//----------------------------------------------------------------------
	// extendKmer
	//----------------------------------------------------------------------
	private boolean extendKmer(Kmer kmer, int index_kmer, int index_last_mod,
			Sequence sequence, Kmer kmerRC, Kmer kmerAux, CandidatePath currentPath, 
			List<CandidatePath> candidatePaths, int org_boundary_left, int org_boundary_right, 
			boolean runExploration) {

		byte[] bases = sequence.getBases();
		byte[] quals = sequence.getQuals();
		byte kmerLength = getKmerLength();
		byte base, baseTemp;
		int multi = 0, it_alter, index;

		logger.debug("extendKmer: index_kmer {}, index_last_mod {}, org_boundary_left {}, org_boundary_right {}, runExploration {}", index_kmer, index_last_mod, org_boundary_left, org_boundary_right, runExploration);

		if (!runExploration)
			return runExploration;

		// generate a new k-mer
		index = index_kmer + kmerLength;
		base = bases[index];
		kmerAux.set(kmer);
		kmerAux.forwardBase(base, kmerLength);
		//logger.trace("kmer {}", kmerAux.basesToString(kmerLength));

		// this was the real boundary between solid k-mers and weak k-mers
		// check all possible cases
		if ((index_kmer == (org_boundary_left - 1)) ||
				(index_kmer == (org_boundary_right - 1)) ||
				((quals[index] - QS_OFFSET) <= QS_EXTREMELY_LOW)) {

			// for each nucleotide
			for (it_alter = Kmer.ABASE; it_alter<Kmer.NUM_NUCLEOTIDES; it_alter++) {
				// make a change
				kmerAux.setBase((byte) it_alter, kmerLength - 1);
				//logger.trace("kmer {}", kmerAux.basesToString(kmerLength));

				multi = getKmerMultiplicity(kmerAux, kmerRC);

				if (multi != 0) {
					// k-mer is solid
					logger.trace("kmer is solid, multi {}", multi);
					baseTemp = Kmer.DECODE[it_alter];

					// generate a new path
					CandidatePath tempPath = new CandidatePath(currentPath);

					// not equal to the original character
					if (base != baseTemp)
						tempPath.addCorrection(new Correction((short) (index), baseTemp));

					// if this k-mer is the last k-mer that can be modified
					// running extend_a_kmer_right is not needed any more
					if ((index_kmer + 1) == index_last_mod) {
						candidatePaths.add(tempPath);

						if (logger.isDebugEnabled()) {
							logger.debug("candidatePath corrections");
							for (Correction corr: tempPath.getModifiedBases()) {
								logger.debug("{} {}", corr.getIndex(), corr.getBase());
							}
						}

						// too many candidate paths
						if (candidatePaths.size() > MAX_CANDIDATE_PATHS)
							runExploration = false;
					} else {
						// trace  this kmer recursively and update candidate_path_vector
						runExploration = extendKmer(kmerAux, index_kmer + 1, index_last_mod,
								sequence, kmerRC, KmerGenerator.createKmer(),
								tempPath, candidatePaths, org_boundary_left, org_boundary_right,
								runExploration);
					}
				}
			}
		} else {
			multi = getKmerMultiplicity(kmerAux, kmerRC);

			if (multi != 0) {
				// k-mer is solid
				logger.trace("kmer is solid, multi {}", multi);

				// if this k-mer is the last k-mer that can be modified
				// running extend_a_kmer_right is not needed any more
				if ((index_kmer + 1) == index_last_mod) {
					candidatePaths.add(currentPath);
					logger.trace("add current path");

					// too many candidate paths
					if (candidatePaths.size() > MAX_CANDIDATE_PATHS)
						runExploration = false;
				} else {
					// trace  this kmer recursively and update candidate_path_vector
					runExploration = extendKmer(kmerAux, index_kmer + 1, index_last_mod,
							sequence, kmerRC, KmerGenerator.createKmer(),
							currentPath, candidatePaths, org_boundary_left, org_boundary_right,
							runExploration);
				}
			} else {
				// for each nucleotide
				for (it_alter = Kmer.ABASE; it_alter<Kmer.NUM_NUCLEOTIDES; it_alter++) {
					baseTemp = Kmer.DECODE[it_alter];

					// not equal to the original character
					if (base != baseTemp) {
						// make a change
						kmerAux.setBase((byte) it_alter, kmerLength - 1);
						//logger.trace("kmer {}", kmerAux.basesToString());

						multi = getKmerMultiplicity(kmerAux, kmerRC);

						if (multi != 0) {
							// k-mer is solid
							logger.trace("kmer is solid, multi {}", multi);

							// generate a new path
							CandidatePath tempPath = new CandidatePath(currentPath);
							tempPath.addCorrection(new Correction((short) (index), baseTemp));

							// if this k-mer is the last k-mer that can be modified
							// running extend_a_kmer_right is not needed any more
							if ((index_kmer + 1) == index_last_mod) {
								candidatePaths.add(tempPath);

								if (logger.isDebugEnabled()) {
									logger.debug("candidatePath corrections");
									for (Correction corr: tempPath.getModifiedBases()) {
										logger.debug("{} {}", corr.getIndex(), corr.getBase());
									}
								}

								// too many candidate paths
								if (candidatePaths.size() > MAX_CANDIDATE_PATHS)
									runExploration = false;
							} else {
								// trace  this kmer recursively and update candidate_path_vector
								runExploration = extendKmer(kmerAux, index_kmer + 1,
										index_last_mod, sequence, kmerRC,
										KmerGenerator.createKmer(), tempPath,
										candidatePaths, org_boundary_left,
										org_boundary_right, runExploration);
							}
						}
					}
				}
			}
		}

		return runExploration;
	}

	//----------------------------------------------------------------------
	// Proceeds correction towards 5' end.
	//----------------------------------------------------------------------
	private boolean extendKmer5PrimeEnd(Kmer kmer, int index_kmer, Sequence sequence, 
			Kmer kmerRC, Kmer kmerAux, CandidatePath currentPath, 
			List<CandidatePath> candidatePaths, int org_boundary, boolean runExploration) {

		byte[] bases = sequence.getBases();
		byte[] quals = sequence.getQuals();
		byte kmerLength = getKmerLength();
		byte base, baseTemp;
		int multi = 0, it_alter;

		logger.debug("extendKmer5PrimeEnd: index_kmer {}, org_boundary {}, runExploration {}", index_kmer, org_boundary, runExploration);

		if (!runExploration)
			return runExploration;

		// generate a new k-mer
		base = bases[index_kmer - 1];
		kmerAux.set(kmer);
		kmerAux.backwardBase(base, kmerLength);
		//logger.trace("kmer {}", kmerAux.basesToString());

		// this was the real boundary between solid k-mers and weak k-mers
		// check all possible cases
		if ((index_kmer == (org_boundary + 1)) ||
				((quals[index_kmer - 1] - QS_OFFSET) <= QS_EXTREMELY_LOW)) {

			// for each nucleotide
			for (it_alter = Kmer.ABASE; it_alter<Kmer.NUM_NUCLEOTIDES; it_alter++) {
				// make a change
				kmerAux.setBase((byte) it_alter, 0);
				//logger.trace("kmer {}", kmerAux.basesToString());

				multi = getKmerMultiplicity(kmerAux, kmerRC);

				if (multi != 0) {
					// k-mer is solid
					logger.trace("kmer is solid, multi {}", multi);
					baseTemp = Kmer.DECODE[it_alter];

					// generate a new path
					CandidatePath tempPath = new CandidatePath(currentPath);

					// not equal to the original character
					if (base != baseTemp)
						tempPath.addCorrection(new Correction((short) (index_kmer - 1), baseTemp));

					// if this k-mer is the first k-mer in a read
					// running extend_a_kmer_5_prime_end is not needed any more
					if ((index_kmer - 1) == 0) {
						candidatePaths.add(tempPath);

						if (logger.isDebugEnabled()) {
							logger.debug("candidatePath corrections");
							for (Correction corr: tempPath.getModifiedBases()) {
								logger.debug("{} {}", corr.getIndex(), corr.getBase());
							}
						}

						// too many candidate paths
						if (candidatePaths.size() > MAX_CANDIDATE_PATHS)
							runExploration = false;
					} else if ((index_kmer - 1) > 0) {
						// trace this kmer recursively and update candidate_path_vector
						runExploration = extendKmer5PrimeEnd(kmerAux, index_kmer - 1,
								sequence, kmerRC, KmerGenerator.createKmer(),
								tempPath, candidatePaths, org_boundary, runExploration);
					}
				}
			}
		} else {
			multi = getKmerMultiplicity(kmerAux, kmerRC);

			if (multi != 0) {
				// k-mer is solid
				logger.trace("kmer is solid, multi {}", multi);

				// if this k-mer is the first k-mer in a read
				// running extend_a_kmer_5_prime_end is not needed any more
				if ((index_kmer - 1) == 0) {
					candidatePaths.add(currentPath);
					logger.trace("add current path");

					// too many candidate paths
					if (candidatePaths.size() > MAX_CANDIDATE_PATHS)
						runExploration = false;
				} else if ((index_kmer - 1) > 0) {
					// trace this kmer recursively and update candidate_path_vector
					runExploration = extendKmer5PrimeEnd(kmerAux, index_kmer - 1, sequence,
							kmerRC, KmerGenerator.createKmer(), currentPath,
							candidatePaths, org_boundary, runExploration);
				}
			} else {
				// for each nucleotide
				for (it_alter = Kmer.ABASE; it_alter<Kmer.NUM_NUCLEOTIDES; it_alter++) {
					baseTemp = Kmer.DECODE[it_alter];

					// not equal to the original character
					if (base != baseTemp) {
						// make a change
						kmerAux.setBase((byte) it_alter, 0);
						//logger.trace("kmer {}", kmerAux.basesToString());

						multi = getKmerMultiplicity(kmerAux, kmerRC);

						if (multi != 0) {
							// k-mer is solid
							logger.trace("kmer is solid, multi {}", multi);

							// generate a new path
							CandidatePath tempPath = new CandidatePath(currentPath);
							tempPath.addCorrection(new Correction((short) (index_kmer - 1), baseTemp));

							// if this k-mer is the first k-mer in a read
							// running extend_a_kmer_5_prime_end is not needed any more
							if ((index_kmer - 1) == 0) {
								candidatePaths.add(tempPath);

								if (logger.isDebugEnabled()) {
									logger.debug("candidatePath corrections");
									for (Correction corr: tempPath.getModifiedBases()) {
										logger.debug("{} {}", corr.getIndex(), corr.getBase());
									}
								}

								// too many candidate paths
								if (candidatePaths.size() > MAX_CANDIDATE_PATHS)
									runExploration = false;
							} else if ((index_kmer - 1) > 0) {
								// trace this kmer recursively and update candidate_path_vector
								runExploration = extendKmer5PrimeEnd(kmerAux, index_kmer - 1,
										sequence, kmerRC, KmerGenerator.createKmer(),
										tempPath, candidatePaths, org_boundary, runExploration);
							}
						}
					}
				}
			}
		}

		return runExploration;
	}

	//----------------------------------------------------------------------
	// Proceeds correction towards 3' end.
	//----------------------------------------------------------------------
	private boolean extendKmer3PrimeEnd(Kmer kmer, int index_kmer, Sequence sequence, 
			Kmer kmerRC, Kmer kmerAux, CandidatePath currentPath, 
			List<CandidatePath> candidatePaths, int org_boundary, boolean runExploration) {

		byte[] bases = sequence.getBases();
		byte[] quals = sequence.getQuals();
		byte kmerLength = getKmerLength();
		int seqLength = sequence.getLength();
		byte base, baseTemp;
		int multi = 0, it_alter, index;

		logger.debug("extendKmer3PrimeEnd: index_kmer {}, org_boundary {}, runExploration {}", index_kmer, org_boundary, runExploration);

		if (!runExploration)
			return runExploration;

		// generate a new k-mer
		index = index_kmer + kmerLength;
		base = bases[index];
		kmerAux.set(kmer);
		kmerAux.forwardBase(base, kmerLength);
		//logger.trace("kmer {}", kmerAux.basesToString(kmerLength));

		// this was the real boundary between solid k-mers and weak k-mers
		// check all possible cases
		if ((index_kmer == (org_boundary - 1)) ||
				((quals[index] - QS_OFFSET) <= QS_EXTREMELY_LOW)) {

			// for each nucleotide
			for (it_alter = Kmer.ABASE; it_alter<Kmer.NUM_NUCLEOTIDES; it_alter++) {
				// make a change
				kmerAux.setBase((byte) it_alter, kmerLength - 1);
				//logger.trace("kmer {}", kmerAux.basesToString(kmerLength));

				multi = getKmerMultiplicity(kmerAux, kmerRC);

				if (multi != 0) {
					// k-mer is solid
					logger.trace("kmer is solid, multi {}", multi);
					baseTemp = Kmer.DECODE[it_alter];

					// generate a new path
					CandidatePath tempPath = new CandidatePath(currentPath);

					// not equal to the original character
					if (base != baseTemp)
						tempPath.addCorrection(new Correction((short) (index), baseTemp));

					// if this k-mer is the first k-mer in a read
					// running extend_a_kmer_3_prime_end is not needed any more
					if ((index_kmer + 1) == (seqLength - kmerLength)) {
						candidatePaths.add(tempPath);

						if (logger.isDebugEnabled()) {
							logger.debug("candidatePath corrections");
							for (Correction corr: tempPath.getModifiedBases()) {
								logger.debug("{} {}", corr.getIndex(), corr.getBase());
							}
						}

						// too many candidate paths
						if (candidatePaths.size() > MAX_CANDIDATE_PATHS)
							runExploration = false;
					} else if ((index_kmer + 1) < (seqLength - kmerLength)) {
						// trace this kmer recursively and update candidate_path_vector
						runExploration = extendKmer3PrimeEnd(kmerAux, index_kmer + 1,
								sequence, kmerRC, KmerGenerator.createKmer(),
								tempPath, candidatePaths, org_boundary, runExploration);
					}
				}
			}
		} else {
			multi = getKmerMultiplicity(kmerAux, kmerRC);

			if (multi != 0) {
				// k-mer is solid
				logger.trace("kmer is solid, multi {}", multi);

				// if this k-mer is the first k-mer in a read
				// running extend_a_kmer_3_prime_end is not needed any more
				if ((index_kmer + 1) == (seqLength - kmerLength)) {
					candidatePaths.add(currentPath);
					logger.trace("add current path");

					// too many candidate paths
					if (candidatePaths.size() > MAX_CANDIDATE_PATHS)
						runExploration = false;
				} else if ((index_kmer + 1) < (seqLength - kmerLength)) {
					// trace this kmer recursively and update candidate_path_vector
					runExploration = extendKmer3PrimeEnd(kmerAux, index_kmer + 1, sequence,
							kmerRC, KmerGenerator.createKmer(), currentPath,
							candidatePaths, org_boundary, runExploration);
				}
			} else {
				// for each nucleotide
				for (it_alter = Kmer.ABASE; it_alter<Kmer.NUM_NUCLEOTIDES; it_alter++) {
					baseTemp = Kmer.DECODE[it_alter];

					// not equal to the original character
					if (base != baseTemp) {
						// make a change
						kmerAux.setBase((byte) it_alter, kmerLength - 1);
						//logger.trace("kmer {}", kmerAux.basesToString());

						multi = getKmerMultiplicity(kmerAux, kmerRC);

						if (multi != 0) {
							// k-mer is solid
							logger.trace("kmer is solid, multi {}", multi);

							// generate a new path
							CandidatePath tempPath = new CandidatePath(currentPath);
							tempPath.addCorrection(new Correction((short) index, baseTemp));

							// if this k-mer is the first k-mer in a read
							// running extend_a_kmer_3_prime_end is not needed any more
							if ((index_kmer + 1) == (seqLength - kmerLength)) {
								candidatePaths.add(tempPath);

								if (logger.isDebugEnabled()) {
									logger.debug("candidatePath corrections");
									for (Correction corr: tempPath.getModifiedBases()) {
										logger.debug("{} {}", corr.getIndex(), corr.getBase());
									}
								}

								// too many candidate paths
								if (candidatePaths.size() > MAX_CANDIDATE_PATHS)
									runExploration = false;
							} else if ((index_kmer + 1) < (seqLength - kmerLength)) {
								// trace  this kmer recursively and update candidate_path_vector
								runExploration = extendKmer3PrimeEnd(kmerAux, index_kmer + 1,
										sequence, kmerRC, KmerGenerator.createKmer(),
										tempPath, candidatePaths, org_boundary, runExploration);
							}
						}
					}
				}
			}
		}

		return runExploration;
	}

	//----------------------------------------------------------------------
	// Checks if the read can be extended towards 5'.
	//----------------------------------------------------------------------
	private void performExtendOutLeft(Kmer kmer, Sequence sequence, byte[] sequence_tmp,
			CandidatePath candidatePath, List<CandidatePath> candidatePaths,
			Kmer kmerRC, Kmer kmerAux) {

		int index_smallest_modified = 0, extend_amount, it_alter;
		byte kmerLength = getKmerLength();
		List<Correction> modifiedBases = candidatePath.getModifiedBases();
		boolean extensionSuccess = false;
		int multi;

		index_smallest_modified = modifiedBases.get(modifiedBases.size() - 1).getIndex();

		logger.debug("performExtendOutLeft: index_smallest_modified {}", index_smallest_modified);

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

		logger.debug("extend_amount {}", extend_amount);

		// generate an initial k-mer
		kmer.set(sequence_tmp, 0, kmerLength);
		kmer.backwardBase((byte)'N', kmerLength);

		// for each nucleotide
		for (it_alter = Kmer.ABASE; it_alter<Kmer.NUM_NUCLEOTIDES; it_alter++) {
			// make a change
			kmer.setBase((byte) it_alter, 0);
			//logger.trace("kmer {}", kmer.basesToString(kmerLength));
			multi = getKmerMultiplicity(kmer, kmerRC);

			if (multi != 0) {
				// k-mer is solid
				logger.trace("multi {}", multi);

				if (extend_amount == 1) {
					// running extend_out_left is not needed any more
					extensionSuccess = true;
					break;
				} else if (!extensionSuccess) {
					// trace this kmer recursively
					extensionSuccess = extendOutLeft(kmer, 1, extend_amount, kmerRC, kmerAux);
				}
			}
		}

		if (extensionSuccess == true) {
			candidatePaths.add(candidatePath);
		}
	}

	//----------------------------------------------------------------------
	// Extends read towards 5'.
	//----------------------------------------------------------------------
	private boolean extendOutLeft(Kmer kmer, int num_extend, int extend_amount, Kmer kmerRC, Kmer kmerAux) {
		int it_alter, multi;
		byte kmerLength = getKmerLength();

		logger.debug("extendOutLeft");

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
							kmerRC, KmerGenerator.createKmer()))
						return true;
				}
			}
		}

		return false;
	}

	//----------------------------------------------------------------------
	// Checks if the read can be extended towards 3'.
	//----------------------------------------------------------------------
	private void performExtendOutRight(Kmer kmer, Sequence sequence, byte[] sequence_tmp,
			CandidatePath candidatePath, List<CandidatePath> candidatePaths,
			Kmer kmerRC, Kmer kmerAux) {

		int index_largest_modified = 0, extend_amount, it_alter;
		byte kmerLength = getKmerLength();
		int seqLength = sequence.getLength();
		int size = seqLength - kmerLength;
		List<Correction> modifiedBases = candidatePath.getModifiedBases();
		boolean extensionSuccess = false;
		int multi;

		index_largest_modified = modifiedBases.get(modifiedBases.size() - 1).getIndex();

		logger.debug("performExtendOutRight: index_largest_modified {}", index_largest_modified);

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

		if (index_largest_modified <= seqLength + MAX_EXTEND - kmerLength) {
			extend_amount = kmerLength - (seqLength - index_largest_modified);
		} else {
			extend_amount = MAX_EXTEND;
		}

		logger.debug("extend_amount {}", extend_amount);

		// generate an initial k-mer
		kmer.set(sequence_tmp, size, kmerLength);
		kmer.forwardBase((byte)'N', kmerLength);

		// for each nucleotide
		for (it_alter = Kmer.ABASE; it_alter<Kmer.NUM_NUCLEOTIDES; it_alter++) {
			// make a change
			kmer.setBase((byte) it_alter,  kmerLength - 1);
			//logger.trace("kmer {}", kmer.basesToString(kmerLength));
			multi = getKmerMultiplicity(kmer, kmerRC);

			if (multi != 0) {
				// k-mer is solid
				logger.trace("multi {}", multi);

				if (extend_amount == 1) {
					// running extend_out_right is not needed any more
					extensionSuccess = true;
					break;
				} else if (!extensionSuccess) {
					// trace this kmer recursively
					extensionSuccess = extendOutRight(kmer, 1, extend_amount, kmerRC, kmerAux);
				}
			}
		}

		if (extensionSuccess == true) {
			candidatePaths.add(candidatePath);
		}
	}

	//----------------------------------------------------------------------
	// Extends read towards 3'.
	//----------------------------------------------------------------------
	private boolean extendOutRight(Kmer kmer, int num_extend, int extend_amount, Kmer kmerRC, Kmer kmerAux) {
		int it_alter, multi;
		byte kmerLength = getKmerLength();

		logger.debug("extendOutRight");

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
					// running extend_out_left is not needed any more
					return true;
				} else {
					// trace this kmer recursively
					if (extendOutRight(kmerAux, num_extend + 1, extend_amount,
							kmerRC, KmerGenerator.createKmer()))
						return true;
				}
			}
		}

		return false;
	}

	//----------------------------------------------------------------------
	// Tries to correct k-mer by changing one symbol.
	//----------------------------------------------------------------------
	private void checkFirstKmer(Kmer kmer, Kmer kmerAux, Kmer kmerRC,
			CandidatePath candidatePath, List<MutablePair<Short,Byte>> lowQsIndexes,
			List<CandidatePath> candidatePaths, int index, int limit) {

		int it_alter, kmer_index, multi;
		CandidatePath candidatePathNext;

		// generate a new k-mer
		kmerAux.set(kmer);
		kmer_index = lowQsIndexes.get(index).getLeft();

		logger.debug("checkFirstKmer: index {}, limit {}, kmer_index {}", index, limit, kmer_index);

		// for each nucleotide
		for (it_alter = Kmer.ABASE; it_alter<Kmer.NUM_NUCLEOTIDES; it_alter++) {	
			// make a new k-mer
			kmerAux.setBase((byte) it_alter, kmer_index);			
			//logger.trace("kmer {}", kmerAux.basesToString());

			candidatePathNext = new CandidatePath(candidatePath);

			if (kmer.getBase(kmer_index) != it_alter)
				candidatePathNext.addCorrection(new Correction((short) (kmer_index), Kmer.DECODE[it_alter]));

			// the max number of low quality base is reached
			// add the path to the vector
			// the original k-mer is also included
			if (index == limit) {
				multi = getKmerMultiplicity(kmerAux, kmerRC);

				if (multi != 0) {
					// k-mer is solid
					logger.trace("multi {}", multi);
					// generate a new path
					candidatePaths.add(candidatePathNext);
				}
			} else {
				// the rightmost low quality base is not reached
				// do recursively
				checkFirstKmer(kmerAux, KmerGenerator.createKmer(),
						kmerRC, candidatePathNext, lowQsIndexes, 
						candidatePaths, index + 1, limit);
			}
		}
	}

	//----------------------------------------------------------------------
	// Checks if modified first k-mer can be extended.
	//----------------------------------------------------------------------
	private boolean solidFirstKmer(Sequence sequence, CandidatePath path, Kmer firstKmer, Kmer kmerRC, Kmer kmerAux) {
		int index_smallest_modified, extend_amount, it_bases, it_alter, multi;
		byte[] bases = sequence.getBases();
		byte kmerLength = getKmerLength();
		List<Correction> modifiedBases = path.getModifiedBases();
		byte base;
		boolean extensionSuccess = false;

		// index_smallest_modified
		index_smallest_modified = modifiedBases.get(0).getIndex();

		logger.debug("solidFirstKmer: index_smallest_modified {}", index_smallest_modified);

		// applied the modified bases to first_kmer
		firstKmer.set(bases, 0, kmerLength);
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

		logger.debug("extend_amount {}", extend_amount);

		// generate an initial k-mer
		kmerAux.set(firstKmer);
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

		logger.debug("extensionSuccess {}", extensionSuccess);

		return extensionSuccess;
	}

	//----------------------------------------------------------------------
	// Proceeds correction from modified first k-mer towards 3' end.
	//----------------------------------------------------------------------
	private boolean extendFirstKmerToRight(Sequence sequence, CandidatePath pathIn, List<CandidatePath> candidatePaths,
			byte[] basesTemp, Kmer kmer, Kmer kmerRC, Kmer kmerAux, boolean runExploration) {
		int it_bases, it_alter, multi, index;
		byte[] bases = sequence.getBases();
		byte kmerLength = getKmerLength();
		int seqLength = sequence.getLength();
		List<Correction> modifiedBases = pathIn.getModifiedBases();
		List<CandidatePath> candidatePathsTemp = new ArrayList<CandidatePath>();
		byte base;

		logger.debug("extendFirstKmerToRight");

		// generate the second k-mer
		kmer.set(bases, 1, kmerLength);
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
				candidatePathsTemp.add(pathIn);
			} else if ((seqLength - kmerLength) > 1) {
				base = Kmer.DECODE[kmer.getBase(kmerLength - 1)];

				// trace this kmer recursively and update candidate_path_vector_tmp
				// org_boundary is not needed in this case: read_length is being used as a dummy number
				runExploration = extendKmer3PrimeEnd(kmer, 1, sequence, kmerRC, kmerAux, pathIn,
						candidatePathsTemp, seqLength, runExploration);
			}
		} else {
			// second_kmer is not solid
			// for each nucleotide
			for (it_alter = Kmer.ABASE; it_alter<Kmer.NUM_NUCLEOTIDES; it_alter++) {
				base = Kmer.DECODE[it_alter];

				// not equal to the original character
				if (bases[kmerLength] != base) {
					// make a change
					kmer.setBase((byte) it_alter, kmerLength - 1);
					//logger.trace("second_kmer {}", kmer.basesToString());
					multi = getKmerMultiplicity(kmer, kmerRC);

					if (multi != 0) {
						// k-mer is solid
						logger.trace("multi {}", multi);

						CandidatePath path = new CandidatePath(pathIn);
						path.addCorrection(new Correction((short) kmerLength, base));

						if ((seqLength - kmerLength) == 1) {
							// if this k-mer is the last k-mer in a read
							// running extend_a_kmer_3_prime_end is not needed any more
							candidatePathsTemp.add(path);
						} else if ((seqLength - kmerLength) > 1) {
							// trace  this kmer recursively and update candidate_path_vector_tmp
							// org_boundary is not needed in this case: read_length is being used as a dummy number
							runExploration = extendKmer3PrimeEnd(kmer, 1, sequence, kmerRC, kmerAux, path,
									candidatePathsTemp, seqLength, runExploration);
						}
					}
				}
			}
		}

		// complete exploration was not done because there are too many candidata paths
		// remove all the paths in candidate_path_vector_tmp
		if (!runExploration)
			candidatePathsTemp.clear();

		// check the solidness of the rightmost k-mers of each modified base
		// for each candidate path
		for (CandidatePath path: candidatePathsTemp)
			performExtendOutRight(kmer, sequence, basesTemp, path, candidatePaths, kmerRC, kmerAux);

		if (logger.isDebugEnabled()) {
			logger.debug("candidatePaths size {}", candidatePaths.size());
			for (CandidatePath path: candidatePaths)
				logger.debug(path.getSumQs() + " " + path.getModifiedBases().size());
		}

		return runExploration;
	}

	private int modifyErrorsFirstKmer(Sequence sequence, List<CandidatePath> candidatePaths,
			Trimming trim, int minCheckLength, int maxTrimmedBases, byte[] sequenceMod) {
		byte[] bases = sequence.getBases();
		byte[] quals = sequence.getQuals();
		int seqLength = sequence.getLength();
		byte base;
		int i, index, numCorrectedErrors = 0, it_mod_base, it_base;
		boolean tooManyCorrections;
		CandidatePath it_path_1st = null;
		List<Correction> modifiedBases;

		logger.debug("modifyErrorsFirstKmer: {} paths", candidatePaths.size());

		if (candidatePaths.size() > 1) {
			int qs_1st = INIT_MIN_QS;
			int qs_2nd = INIT_MIN_QS;

			// for each candidate path
			for (CandidatePath path: candidatePaths) {
				// each modification
				for (Correction corr: path.getModifiedBases()) {
					if (bases[corr.getIndex()] != corr.getBase()) {
						// add quality scores of modified bases
						path.incQuality(quals[corr.getIndex()] - QS_OFFSET);
					}
				}

				// compare quality scores of each path
				if (path.getSumQs() <= qs_1st) {
					qs_2nd = qs_1st;
					qs_1st = path.getSumQs();
					it_path_1st = path;
				}
				else if (path.getSumQs() <= qs_2nd) {
					qs_2nd = path.getSumQs();
				}
			}

			if (logger.isDebugEnabled()) {
				logger.debug("candidatePaths size {}", candidatePaths.size());
				for (CandidatePath path: candidatePaths)
					logger.debug(path.getSumQs() + " " +path.getModifiedBases().size());
			}

			// check whether too many bases are modified in the first path, which implies indel may exist in the original read
			// if too many modifications exist, the read is just trimmed
			tooManyCorrections = false;
			modifiedBases = it_path_1st.getModifiedBases();
			int numBases = modifiedBases.size();
			int result;

			// at least over MAX_MODIFICATION times of modifications from the first modified position
			if (((seqLength - modifiedBases.get(0).getIndex()) >= minCheckLength) &&
					(numBases > MAX_MODIFICATION)) {
				// each modified base in a right range
				for (it_mod_base = 0; it_mod_base < numBases - MAX_MODIFICATION; it_mod_base++) {

					result = modifiedBases.get(it_mod_base + MAX_MODIFICATION).getIndex() - modifiedBases.get(it_mod_base).getIndex();

					// at least MAX_MODIFICATION times of modifications within min_check_length bases
					if (Integer.compareUnsigned(result, minCheckLength) < 0) {
						// trim_5_end or trim_3_end
						if ((seqLength - modifiedBases.get(it_mod_base).getIndex()) <= maxTrimmedBases)
							trim.set3End(seqLength - modifiedBases.get(it_mod_base).getIndex());
						else if ((modifiedBases.get(it_mod_base).getIndex() + 1) <= maxTrimmedBases)
							trim.set5End(modifiedBases.get(it_mod_base).getIndex() + 1);

						tooManyCorrections = true;
						break;
					}
				}
			}

			logger.debug("tooManyCorrections {}, trim3End {}, trim5End {}", tooManyCorrections, trim.get3End(), trim.get5End());

			// use the 1st path if not too many corrections are made AND if the 1st path has a sufficiently low score
			// if an indel exists using quality scores is not a good way to choose the best path
			if ((tooManyCorrections == false) && (qs_1st + MIN_QS_DIFF <= qs_2nd)) {
				// update sequence_modification
				for (Correction corr: modifiedBases) {
					index = corr.getIndex();
					base = corr.getBase();
					sequenceMod[index] = base;
					bases[index] = base;
					logger.debug("Correction(1): index {} base {}", index, base);
					numCorrectedErrors++;
				}
			} else {
				// hard to choose one path
				// A AND B
				List<Correction> baseIntersection = new ArrayList<Correction>();
				// A OR B
				List<Correction> baseUnion = new ArrayList<Correction>();
				// A - B
				List<Correction> baseDifference = new ArrayList<Correction>();

				List<Correction> baseIntersectionPrev = new ArrayList<Correction>(candidatePaths.get(0).getModifiedBases());
				List<Correction> baseUnionPrev = new ArrayList<Correction>(candidatePaths.get(0).getModifiedBases());

				// each candidate path
				for (i = 1; i < candidatePaths.size(); i++) {
					baseIntersection.clear();
					baseUnion.clear();

					baseIntersection(baseIntersectionPrev, candidatePaths.get(i).getModifiedBases(), baseIntersection);
					baseUnion(baseUnionPrev, candidatePaths.get(i).getModifiedBases(), baseUnion);

					baseIntersectionPrev.clear();
					baseIntersectionPrev.addAll(baseIntersection);
					baseUnionPrev.clear();
					baseUnionPrev.addAll(baseUnion);
				}

				baseDifference(baseUnion, baseIntersection, baseDifference);

				if (logger.isDebugEnabled()) {
					logger.debug("baseIntersection size {}", baseIntersection.size());
					for (Correction corr: baseIntersection)
						logger.debug(corr.getIndex() + "," +corr.getBase());
					logger.debug("baseUnion size {}", baseUnion.size());
					for (Correction corr: baseUnion)
						logger.debug(corr.getIndex() + "," +corr.getBase());
					logger.debug("baseDifference size {}", baseDifference.size());
					for (Correction corr: baseDifference)
						logger.debug(corr.getIndex() + "," +corr.getBase());
				}

				// find trimmed region
				// correcting the 5'-end and 3'-end was not done
				// therefore the total number of trimmed bases is 0 yet
				if (baseDifference.size() > 0) {
					boolean keepGoing = true;
					int indexLeftmost = 0;
					int indexRightmost = baseDifference.size() - 1;
					Correction leftmost, rightmost;

					while (keepGoing) {
						leftmost = baseDifference.get(indexLeftmost);
						rightmost = baseDifference.get(indexRightmost);

						// # of trimmed bases at the 5' end: base_vector_difference[vector_index_leftmost].first + 1
						// # of trimmed bases at the 3' end: read_length - base_vector_difference[vector_index_rightmost].first
						if ((leftmost.getIndex() + 1) < (seqLength - rightmost.getIndex())) {
							// the 5'-end is smaller
							// check the total number of trimmed bases
							if ((leftmost.getIndex() + 1 + trim.get3End()) <= maxTrimmedBases) {
								trim.set5End(leftmost.getIndex() + 1);

								// two points are met
								if (indexLeftmost == indexRightmost) {
									keepGoing = false;
								} else {
									indexLeftmost++;
								}
							} else {
								// no need for more check
								keepGoing = false;
							}
						} else {
							// the 3'-end is smaller
							// check the total number of trimmed bases
							if ((seqLength - rightmost.getIndex()) <= maxTrimmedBases) {
								trim.set3End(seqLength - rightmost.getIndex());

								// two points are met
								if (indexLeftmost == indexRightmost) {
									keepGoing = false;
								} else {
									indexRightmost--;
								}
							} else {
								// no need for more check
								keepGoing = false;
							}
						}
					}

					logger.debug("trim3End {}, trim5End {}", trim.get3End(), trim.get5End());
				}

				// find consensus modifications
				for (Correction corr: baseIntersection) {
					// check whether the base is not in the trimmed regions
					if ((corr.getIndex() <  (seqLength - trim.get3End())) &&
							(corr.getIndex() >= trim.get5End())) {
						index = corr.getIndex();
						base = corr.getBase();

						// filter out the bases that are equal to the original ones
						if (bases[index] != base) {
							sequenceMod[index] = base;
							bases[index] = base;
							logger.debug("Correction(2): index {} base {}", index, base);
							numCorrectedErrors++;
						}
					}
				}
			}
		} else if (candidatePaths.size() == 1) {
			// only one path
			// check whether too many bases are modified in the first path, which implies indel may exist in the original read
			// if too many modifications exist, the read is just trimmed
			tooManyCorrections = false;

			// at least over MAX_MODIFICATION times of modifications from the first modified position
			modifiedBases = candidatePaths.get(0).getModifiedBases();
			int numBases = modifiedBases.size();
			int result;

			// at least over MAX_MODIFICATION times of modifications from the first modified position
			if ((seqLength - (modifiedBases.get(0).getIndex()) >= minCheckLength) &&
					(numBases > MAX_MODIFICATION)) {
				// each modified base in a right range
				for (it_mod_base = 0; it_mod_base < numBases - MAX_MODIFICATION; it_mod_base++) {

					result = modifiedBases.get(it_mod_base + MAX_MODIFICATION).getIndex() - modifiedBases.get(it_mod_base).getIndex();

					// at least MAX_MODIFICATION times of modifications within min_check_length bases
					if (Integer.compareUnsigned(result, minCheckLength) < 0) {
						// trim_5_end or trim_3_end
						if ((seqLength - modifiedBases.get(it_mod_base).getIndex()) <= maxTrimmedBases) {
							trim.set3End(seqLength - modifiedBases.get(it_mod_base).getIndex());

							// update sequence_modification for the non-trimmed corrections
							if (it_mod_base > 0) {
								for (it_base = 0; it_base < (it_mod_base - 1); it_base++) {
									index = modifiedBases.get(it_base).getIndex();
									base = modifiedBases.get(it_base).getBase();
									sequenceMod[index] = base;
									bases[index] = base;
									logger.debug("Correction(3): index {} base {}", index, base);
									numCorrectedErrors++;
								}
							}
						} else if ((modifiedBases.get(it_mod_base).getIndex() + 1) <= maxTrimmedBases) {
							trim.set5End(modifiedBases.get(it_mod_base).getIndex() + 1);

							// update sequence_modification for the non-trimmed corrections
							if (it_mod_base < (numBases - 1)) {
								for (it_base = (it_mod_base + 1); it_base < numBases; it_base++) {
									index = modifiedBases.get(it_base).getIndex();
									base = modifiedBases.get(it_base).getBase();
									sequenceMod[index] = base;
									bases[index] = base;
									logger.debug("Correction(4): index {} base {}", index, base);
									numCorrectedErrors++;
								}
							}
						}

						tooManyCorrections = true;
						break;
					}
				}
			}

			logger.debug("tooManyCorrections {}, trim3End {}, trim5End {}", tooManyCorrections, trim.get3End(), trim.get5End());

			if (tooManyCorrections == false) {
				// not too many modified bases
				// update sequence_modification
				for (Correction corr: candidatePaths.get(0).getModifiedBases()) {
					index = corr.getIndex();
					base = corr.getBase();
					sequenceMod[index] = base;
					bases[index] = base;
					logger.debug("Correction(5): index {} base {}", index, base);
					numCorrectedErrors++;
				}
			}
		}

		return numCorrectedErrors;
	}

	private void baseIntersection(List<Correction> in1, List<Correction> in2, List<Correction> out) {
		int in1Size = in1.size();
		int in2Size = in2.size();

		if (in1Size == 0 || in2Size == 0)
			return;

		int it1 = 0, it2 = 0;
		Correction corrIn1;
		Correction corrIn2;

		while (it1 < in1Size && it2 < in2Size) {
			corrIn1 = in1.get(it1);
			corrIn2 = in2.get(it2);

			if (corrIn1.getIndex() < corrIn2.getIndex()) {
				it1++;
			} else if (corrIn2.getIndex() < corrIn1.getIndex()) {
				it2++;
			} else {
				if (corrIn1.getBase() == corrIn2.getBase())
					out.add(corrIn1);

				it1++;
				it2++;
			}
		}
	}

	private void baseUnion(List<Correction> in1, List<Correction> in2, List<Correction> out) {
		int in1Size = in1.size();
		int in2Size = in2.size();

		if (in1Size == 0) {
			if (in2Size != 0)
				out.addAll(in2);

			return;
		}

		if (in2Size == 0) {
			out.addAll(in1);
			return;
		}

		int it1 = 0, it2 = 0;
		Correction corrIn1;
		Correction corrIn2;

		while (true) {
			if (it1 == in1Size) {
				for (int i = it2; i < in2Size; i++)
					out.add(in2.get(i));

				return;
			}

			if (it2 == in2Size) {
				for (int i = it1; i < in1Size; i++)
					out.add(in1.get(i));

				return;
			}

			corrIn1 = in1.get(it1);
			corrIn2 = in2.get(it2);

			if (corrIn1.getIndex() < corrIn2.getIndex()) {
				out.add(corrIn1);
				it1++;
			} else if (corrIn2.getIndex() < corrIn1.getIndex()) {
				out.add(corrIn2);
				it2++;
			} else {
				out.add(corrIn1);
				it1++;
				it2++;
			}
		}
	}

	private void baseDifference(List<Correction> in1, List<Correction> in2, List<Correction> out) {
		int in1Size = in1.size();
		int in2Size = in2.size();

		if (in1Size == 0)
			return;

		if (in2Size == 0) {
			out.addAll(in1);
			return;
		}

		int it1 = 0, it2 = 0;
		Correction corrIn1;
		Correction corrIn2;

		while (it1 < in1Size && it2 < in2Size) {
			corrIn1 = in1.get(it1);
			corrIn2 = in2.get(it2);

			if (corrIn1.getIndex() < corrIn2.getIndex()) {
				out.add(corrIn1);
				it1++;
			} else if (corrIn2.getIndex() < corrIn1.getIndex()) {
				it2++;
			} else {
				it1++;
				it2++;
			}
		}

		for (int i = it1; i < in1Size; i++)
			out.add(in1.get(i));
	}

	private List<MutablePair<Short,Byte>> sortIndexes(byte[] quals, byte kmerLength) {
		List<MutablePair<Short,Byte>> indexes = new ArrayList<MutablePair<Short,Byte>>(kmerLength);

		for (short i = 0; i < kmerLength; i++)
			indexes.add(new MutablePair<Short,Byte>(i, quals[i]));

		// Sort list by quality
		indexes.sort(new Comparator<MutablePair<Short,Byte>>() {
			@Override
			public int compare(MutablePair<Short,Byte> p1, MutablePair<Short,Byte> p2) {
				return p1.getRight().compareTo(p2.getRight());
			}
		});

		return indexes;
	}
}
