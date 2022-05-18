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
package es.udc.gac.bigdec.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import es.udc.gac.bigdec.ec.CorrectionAlgorithm;
import es.udc.gac.bigdec.ec.ErrorCorrection;
import es.udc.gac.bigdec.ec.bless2.BLESS2;
import es.udc.gac.bigdec.ec.musket.MUSKET;
import es.udc.gac.bigdec.ec.reckoner.RECKONER;

public final class Configuration {

	public static final String VERSION = "v1.1";
	public static final String WEBPAGE = "https://github.com/UDC-GAC/BigDEC";
	public static final String BIGDEC_HOME;
	public static final String SLASH;
	public static final int MIN_SPLIT_SIZE_MiB = 1;		// 1 MiB
	public static final long MIN_SPLIT_SIZE = MIN_SPLIT_SIZE_MiB*1024*1024;
	private static final Logger logger = LoggerFactory.getLogger(Configuration.class);

	private List<CorrectionAlgorithm> correctionAlgorithms;
	private String ALGORITHMS = "MUSKET";
	public Short KMER_THRESHOLD = 0;
	public Boolean KEEP_ORDER = true;
	public Boolean MULTITHREAD_MERGE = true;

	public Integer MUSKET_MAX_ITERS = 2;
	public Integer MUSKET_MAX_ERRORS = 4;
	public Integer MUSKET_MAX_TRIM = 0;

	public Integer BLESS2_MAX_KMER_THRESHOLD = 255;
	public Integer BLESS2_MAX_EXTEND = 5;
	public Boolean BLESS2_TRIMMING = true;
	public Double BLESS2_MAX_TRIMMING_RATE = 0.6;
	public Double BLESS2_MAX_N_RATIO = 0.1;
	public Double BLESS2_MAX_ERROR_RATIO = 0.25;
	public String BLESS2_SUBST_BASE = "A";

	public Byte BLESS2_PHRED33 = 33;
	public Byte BLESS2_PHRED64 = 64;
	public Double BLESS2_QS_CUTOFF_RATIO = 0.05;
	public Double BLESS2_QS_EXTREMELY_LOW_RATIO = 0.01;
	public Double BLESS2_CHECK_RANGE_RATIO = 0.07;	
	public Integer BLESS2_MIN_BASES_AFTER_TRIMMING = 30;
	public Integer BLESS2_MIN_SOLID_LENGTH = 2;
	public Integer BLESS2_MIN_NON_SOLID_LENGTH = 2;
	public Integer BLESS2_FP_SUSPECT_LENGTH = 1;
	public Integer BLESS2_SOLID_REGION_ADJUST_RANGE = 4;
	public Integer BLESS2_MAX_CANDIDATE_PATHS = 74;
	public Integer BLESS2_INIT_MIN_QS = 1000000;
	public Integer BLESS2_MAX_MODIFICATION = 4;
	public Integer BLESS2_MIN_QS_DIFF = 10;
	public Integer BLESS2_NUM_ALLOWABLE_FAILS = 2;
	public Integer BLESS2_MAX_LOW_QS_BASES = 3;

	public Integer RECKONER_MAX_KMER_THRESHOLD = 255;
	public Integer RECKONER_MAX_EXTEND = 5;
	public Byte RECKONER_QS_CUTOFF = 10;
	public Double RECKONER_MAX_N_RATIO = 0.3;
	public Double RECKONER_MAX_ERROR_RATIO = 0.5;
	public String RECKONER_SUBST_BASE = "A";

	public Byte RECKONER_PHRED33 = 33;
	public Byte RECKONER_PHRED64 = 64;
	public Byte RECKONER_MIN_33_SCORE = 33;
	public Byte RECKONER_MAX_64_SCORE = 104;
	public Integer RECKONER_MIN_SOLID_LENGTH = 2;
	public Integer RECKONER_MIN_NON_SOLID_LENGTH = 2;
	public Integer RECKONER_MAX_DETECT_SCORE_DIFF = 5;
	public Integer RECKONER_FP_SUSPECT_LENGTH = 1;
	public Integer RECKONER_SOLID_REGION_ADJUST_RANGE = 4;
	public Integer RECKONER_MAX_EXTEND_CORRECTION_PATHS = 100;
	public Integer RECKONER_CHECK_MAX_CHANGES = 1000;
	public Integer RECKONER_MAX_FIRST_KMER_POSSIBILITIES = 5;
	public Integer RECKONER_MAX_FIRST_KMER_CORRECTION_PATHS = 30;
	public Integer RECKONER_MAX_LOW_QS_BASES = 4;
	public Integer RECKONER_MAX_LOW_QS_INDEXES_COMB = 6;
	public Double RECKONER_COVERING_KMERS_WEIGHT = 1.0;
	public Double RECKONER_EXTENSION_KMERS_WEIGHT = 0.5;
	public Double RECKONER_MIN_BEST_KMER_QUALITY = 0.0;
	public Double RECKONER_MAX_CHANGES_IN_REGION_RATIO = 0.5;
	public Boolean RECKONER_LIMIT_MODIFICATIONS = false;

	public String SPARK_API = "Dataset";
	public Boolean SPARK_SERIALIZE_RDD = true;
	public Boolean SPARK_COMPRESS_DATA = false;
	public String SPARK_COMPRESSION_CODEC = "lz4";
	public Integer SPARK_SHUFFLE_PARTITIONS = 1;
	public String FLINK_API = "Dataset";
	public Boolean FLINK_OBJECT_REUSE = true;
	public Boolean FLINK_MULTIPLE_JOB = true;
	public Boolean FLINK_WRITE_KMERS = false;
	public Boolean FLINK_PAIRED_MODE = true;
	public Boolean FLINK_PRE_SHUFFLE_AGGREGATOR = true;
	public Integer FLINK_PRE_SHUFFLE_AGGREGATOR_LIMIT = 262144;
	public Boolean HDFS_DELETE_TEMP_FILES = false;
	public Short HDFS_BLOCK_REPLICATION = 1;

	static {
		Map<String,String> map = System.getenv();
		BIGDEC_HOME = map.get("BIGDEC_HOME");

		if(BIGDEC_HOME == null)
			throw new RuntimeException("'BIGDEC_HOME' must be set");

		SLASH = System.getProperty("file.separator");
		String LOG_GILE = BIGDEC_HOME+SLASH+"conf"+SLASH+"log4j.properties";

		try {
			Properties p = new Properties();
			p.load(new FileInputStream(LOG_GILE));
			PropertyConfigurator.configure(p);
		} catch (FileNotFoundException e) {
			IOUtils.error(e.getMessage());
		} catch (IOException e) {
			IOUtils.error(e.getMessage());
		}

		logger.debug("BIGDEC_HOME = {}", BIGDEC_HOME);
	}

	public Configuration() {
		correctionAlgorithms = null;
	}

	public List<CorrectionAlgorithm> getCorrectionAlgorithms(byte kmerLength) {
		if (correctionAlgorithms != null)
			return correctionAlgorithms;

		correctionAlgorithms = new ArrayList<CorrectionAlgorithm>();
		List<String> list = Arrays.asList(ALGORITHMS.split(","));

		for(String algorithm: list) {
			if (algorithm.equalsIgnoreCase("BLESS2"))
				correctionAlgorithms.add(new BLESS2(this, kmerLength, list.size()));
			if (algorithm.equalsIgnoreCase("RECKONER"))
				correctionAlgorithms.add(new RECKONER(this, kmerLength, list.size()));
			if (algorithm.equalsIgnoreCase("MUSKET"))
				correctionAlgorithms.add(new MUSKET(this, kmerLength, list.size()));
		}
		return correctionAlgorithms;
	}

	public void printConfig() {
		IOUtils.info("############# CONFIG #############");
		try {
			List<Field> banneds = Arrays.asList(
					new Field[] { this.getClass().getDeclaredField("VERSION"),
							this.getClass().getDeclaredField("WEBPAGE"),
							this.getClass().getDeclaredField("SLASH"),
							this.getClass().getDeclaredField("logger"),
							this.getClass().getDeclaredField("MIN_SPLIT_SIZE"),
							this.getClass().getDeclaredField("MIN_SPLIT_SIZE_MiB"),
							this.getClass().getDeclaredField("correctionAlgorithms")});

			Field[] fields = this.getClass().getDeclaredFields();
			for (Field f : fields) {
				if (!banneds.contains(f)) {
					if (!f.getName().contains("RECKONER_") && !f.getName().contains("BLESS2_")
							&& !f.getName().contains("MUSKET_"))
						IOUtils.info(String.format("%s = %s", f.getName(), f.get(this).toString()));
				}
			}
		} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
			IOUtils.error("Error when printing configuration");
		}
		IOUtils.info("##################################");
	}

	public void readConfig(String filePath) {
		Properties configFile = new Properties();

		try {
			InputStream stream = new FileInputStream(filePath);
			configFile.load(stream);

			List<Field> banneds = Arrays.asList(
					new Field[] { this.getClass().getDeclaredField("VERSION"),
							this.getClass().getDeclaredField("WEBPAGE"),
							this.getClass().getDeclaredField("SLASH"),
							this.getClass().getDeclaredField("logger"),
							this.getClass().getDeclaredField("MIN_SPLIT_SIZE"),
							this.getClass().getDeclaredField("MIN_SPLIT_SIZE_MiB"),
							this.getClass().getDeclaredField("correctionAlgorithms")});

			Field[] fields = this.getClass().getDeclaredFields();
			for (Field f : fields) {
				if (!banneds.contains(f)) {
					String v = configFile.getProperty(f.getName());
					if (v != null) {
						f.setAccessible(true);
						if (f.getType() == String.class) {
							f.set(this, v);
						} else if (f.getType() == Boolean.class) {
							f.set(this, Boolean.parseBoolean(v));
						} else if (f.getType() == Integer.class) {
							f.set(this, Integer.parseInt(v));
						} else if (f.getType() == Short.class) {
							f.set(this, Short.parseShort(v));
						} else if (f.getType() == Double.class) {
							f.set(this, Double.parseDouble(v));
						} else if (f.getType() == Byte.class) {
							f.set(this, Byte.parseByte(v));
						}
					}
				}
			}
		} catch (IOException | NoSuchFieldException | SecurityException | IllegalArgumentException
				| IllegalAccessException e) {
			IOUtils.error("Error parsing config file: "+filePath+" ("+e.getLocalizedMessage()+")");
		}

		// Sanity checks
		List<String> list = Arrays.asList(ALGORITHMS.split(","));

		for(String algorithm: list) {
			if (!algorithm.equalsIgnoreCase("BLESS2") &&
					!algorithm.equalsIgnoreCase("BLESS") && 
					!algorithm.equalsIgnoreCase("RECKONER") &&
					!algorithm.equalsIgnoreCase("MUSKET"))
				throw new RuntimeException("ALGORITHMS="+ALGORITHMS+" is not valid. Supported values: MUSKET, BLESS2 and RECKONER");
		}

		if (KMER_THRESHOLD != 0 && KMER_THRESHOLD < 2) {
			IOUtils.warn("The k-mer threshold ("+KMER_THRESHOLD+") must be >= 2. Actual value will be autocalculated");
			KMER_THRESHOLD = 0;
		}

		if (KMER_THRESHOLD > ErrorCorrection.KMER_MAX_COUNTER) {
			IOUtils.warn("The k-mer threshold ("+KMER_THRESHOLD+") must be <= "+ErrorCorrection.KMER_MAX_COUNTER+". Actual value will be autocalculated");
			KMER_THRESHOLD = 0;
		}

		if (BLESS2_MAX_KMER_THRESHOLD < 2) {
			throw new RuntimeException("The maximum k-mer threshold for BLESS2 ("+BLESS2_MAX_KMER_THRESHOLD+") must be >= 2");
		}

		if (BLESS2_MAX_KMER_THRESHOLD > ErrorCorrection.KMER_MAX_COUNTER) {
			throw new RuntimeException("The maximum k-mer threshold for BLESS2 ("+BLESS2_MAX_KMER_THRESHOLD+") must be <= "+ErrorCorrection.KMER_MAX_COUNTER+". Actual value will be autocalculated");
		}

		if (BLESS2_MAX_EXTEND < 1) {
			throw new RuntimeException("The maximum read extension for BLESS2 ("+BLESS2_MAX_EXTEND+") must be >= 1");
		}

		if (BLESS2_MAX_N_RATIO <= 0 || BLESS2_MAX_N_RATIO > 1) {
			throw new RuntimeException("The ratio of 'N' bases for BLESS2 ("+BLESS2_MAX_N_RATIO+") must be (0,1]");
		}

		if ((BLESS2_SUBST_BASE.length() > 1) || (!BLESS2_SUBST_BASE.equalsIgnoreCase("A") && 
				!BLESS2_SUBST_BASE.equalsIgnoreCase("C") &&
				!BLESS2_SUBST_BASE.equalsIgnoreCase("G") &&
				!BLESS2_SUBST_BASE.equalsIgnoreCase("T"))) {
			throw new RuntimeException("The substitution base for BLESS2 ("+BLESS2_SUBST_BASE+") must be: A, C, G or T");
		}

		if (MUSKET_MAX_TRIM < 0) {
			throw new RuntimeException("The maximum trimming value for MUSKET ("+MUSKET_MAX_TRIM+") must be >= 0");
		}

		if (MUSKET_MAX_ITERS < 1) {
			throw new RuntimeException("The maximum number of iterations for MUSKET ("+MUSKET_MAX_ITERS+") must be >= 1");
		}

		if (MUSKET_MAX_ERRORS < 2) {
			throw new RuntimeException("The maximum number of corrected errors for MUSKET ("+MUSKET_MAX_ERRORS+") must be >= 2");
		}

		if (RECKONER_MAX_KMER_THRESHOLD < 2) {
			throw new RuntimeException("The maximum k-mer threshold for RECKONER ("+RECKONER_MAX_KMER_THRESHOLD+") must be >= 2");
		}

		if (RECKONER_MAX_KMER_THRESHOLD > ErrorCorrection.KMER_MAX_COUNTER) {
			throw new RuntimeException("The maximum k-mer threshold for RECKONER ("+RECKONER_MAX_KMER_THRESHOLD+") must be <= "+ErrorCorrection.KMER_MAX_COUNTER+". Actual value will be autocalculated");
		}

		if (RECKONER_MAX_EXTEND < 1) {
			throw new RuntimeException("The maximum read extension for RECKONER ("+RECKONER_MAX_EXTEND+") must be >= 1");
		}

		if (RECKONER_MIN_SOLID_LENGTH < 2) {
			throw new RuntimeException("The minimum length of a solid k-mer for RECKONER ("+RECKONER_MIN_SOLID_LENGTH+") must be >= 2");
		}

		if (RECKONER_MIN_NON_SOLID_LENGTH < 2) {
			throw new RuntimeException("The minimum length of a solid k-mer for RECKONER ("+RECKONER_MIN_NON_SOLID_LENGTH+") must be >= 2");
		}

		if (RECKONER_QS_CUTOFF < 0 || RECKONER_QS_CUTOFF > ErrorCorrection.QS_HISTOGRAM_SIZE) {
			throw new RuntimeException("The quality score threshold for RECKONER ("+RECKONER_QS_CUTOFF+") must be [0,"+ErrorCorrection.QS_HISTOGRAM_SIZE+"]");
		}

		if (RECKONER_MAX_N_RATIO <= 0 || RECKONER_MAX_N_RATIO > 1) {
			throw new RuntimeException("The ratio of 'N' bases for RECKONER ("+RECKONER_MAX_N_RATIO+") must be (0,1]");
		}

		if (RECKONER_MAX_ERROR_RATIO <= 0 || RECKONER_MAX_ERROR_RATIO > 1) {
			throw new RuntimeException("The ratio of corrected bases for RECKONER ("+RECKONER_MAX_ERROR_RATIO+") must be (0,1]");
		}

		if ((RECKONER_SUBST_BASE.length() > 1) || (!RECKONER_SUBST_BASE.equalsIgnoreCase("A") && 
				!RECKONER_SUBST_BASE.equalsIgnoreCase("C") &&
				!RECKONER_SUBST_BASE.equalsIgnoreCase("G") &&
				!RECKONER_SUBST_BASE.equalsIgnoreCase("T"))) {
			throw new RuntimeException("The substitution base for RECKONER ("+RECKONER_SUBST_BASE+") must be: A, C, G or T");
		}

		if (HDFS_BLOCK_REPLICATION < 1) {
			throw new RuntimeException("HDFS_BLOCK_REPLICATION="+HDFS_BLOCK_REPLICATION+" must be >= 1");
		}

		if (!SPARK_API.equalsIgnoreCase("RDD") && !SPARK_API.equalsIgnoreCase("Dataset"))
			throw new RuntimeException("SPARK_API="+SPARK_API+" is invalid. Supported values: RDD and Dataset");

		if (!SPARK_COMPRESSION_CODEC.equalsIgnoreCase("lz4") && !SPARK_COMPRESSION_CODEC.equalsIgnoreCase("snappy"))
			throw new RuntimeException("SPARK_COMPRESSION_CODEC="+SPARK_COMPRESSION_CODEC+" is invalid. Supported values: lz4 and snappy");

		if (SPARK_SHUFFLE_PARTITIONS <= 0) {
			throw new RuntimeException("SPARK_SHUFFLE_PARTITIONS="+SPARK_SHUFFLE_PARTITIONS+" must be >= 1");
		}

		if (FLINK_PRE_SHUFFLE_AGGREGATOR_LIMIT <= 0)
			throw new RuntimeException("FLINK_PRE_SHUFFLE_AGGREGATOR_LIMIT="+FLINK_PRE_SHUFFLE_AGGREGATOR_LIMIT+" must be >= 1");

		if (!FLINK_API.equalsIgnoreCase("Dataset") && !FLINK_API.equalsIgnoreCase("Datastream"))
			throw new RuntimeException("FLINK_API="+FLINK_API+" is invalid. Supported values: Dataset and Datastream");
	}
}
