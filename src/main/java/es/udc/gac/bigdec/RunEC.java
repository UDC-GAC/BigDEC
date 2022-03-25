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
package es.udc.gac.bigdec;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.udc.gac.bigdec.ec.ErrorCorrection;
import es.udc.gac.bigdec.ec.flink.ds.FlinkDS;
import es.udc.gac.bigdec.ec.spark.ds.SparkDS;
import es.udc.gac.bigdec.ec.spark.rdd.SparkRDD;
import es.udc.gac.bigdec.util.CLIOptions;
import es.udc.gac.bigdec.util.Configuration;
import es.udc.gac.bigdec.util.IOUtils;
import es.udc.gac.bigdec.util.Timer;

public class RunEC {

	public enum ExecutionEngine {
		SPARK_MODE, FLINK_MODE
	}

	public static final String APP_NAME = "BigDEC";
	public static ExecutionEngine EXECUTION_ENGINE;

	private static final Logger logger = LoggerFactory.getLogger(RunEC.class);
	private static final Timer timer = new Timer();
	private static final String TOTAL_TIME = "TOTAL_TIME";
	private static final String INIT_TIME = "INIT_TIME";
	private static final String CORRECTION_TIME = "CORRECTION_TIME";
	private static final String MERGE_TIME = "MERGE_TIME";

	public static void main(String[] args) throws Exception {
		Path inputPath1 = null, inputPath2 = null;
		ErrorCorrection ec = null;
		FileSystem fs;
		long nsplits;

		timer.start(TOTAL_TIME);

		// Intialization
		timer.start(INIT_TIME);

		// Load configuration
		Configuration config = new Configuration();
		
		IOUtils.info(RunEC.APP_NAME+" "+Configuration.VERSION);
		logger.debug("args = {}", Arrays.toString(args));

		// Parse command-line options
		CLIOptions options = new CLIOptions();
		options.parse(Arrays.copyOfRange(args, 1, args.length));

		IOUtils.info("k-mer length = "+options.getKmerLength());

		if (options.getConfigPath() != null) {
			IOUtils.info("config path = "+options.getConfigPath());
			config.readConfig(options.getConfigPath());
		}

		IOUtils.info("merge = "+options.getMerge());
		IOUtils.info("merger thread = "+options.runMergerThread());

		// Show configuration
		config.printConfig();

		if (args[0].equalsIgnoreCase("spark")) {
			EXECUTION_ENGINE = ExecutionEngine.SPARK_MODE;

			if (config.SPARK_API.equalsIgnoreCase("RDD")) {
				IOUtils.info("SPARK MODE - RDD");
				ec = new SparkRDD(config, options);
			} else {
				IOUtils.info("SPARK MODE - Dataset");
				ec = new SparkDS(config, options);
			}
		} else if (args[0].equalsIgnoreCase("flink")) {
			EXECUTION_ENGINE = ExecutionEngine.FLINK_MODE;

			if (config.FLINK_API.equalsIgnoreCase("Dataset")) {
				IOUtils.info("FLINK MODE - Dataset");
				ec = new FlinkDS(config, options);
			} else {
				IOUtils.error("Invalid Flink API: "+config.FLINK_API);
			}
		} else {
			IOUtils.error("Invalid execution mode: "+args[0]);
		}

		// Get Hadoop configuration
		org.apache.hadoop.conf.Configuration hadoopConfig = ec.getHadoopConfig();

		// Determine the number of input splits
		nsplits = IOUtils.getNumberOfSplits(hadoopConfig, options, config, ec.getParallelism());

		// Configure HSP library
		es.udc.gac.hadoop.sequence.parser.util.Configuration.setTrimSequenceName(hadoopConfig, false);

		// Get source file system
		fs = IOUtils.getSrcFS();

		// Get input path for the first dataset
		inputPath1 = fs.resolvePath(new Path(options.getInputFile1()));

		if (options.isPaired()) {
			// Get input path for the second dataset
			inputPath2 = fs.resolvePath(new Path(options.getInputFile2()));
		}

		timer.stop(INIT_TIME);

		/*
		 * Core correction phase
		 */
		timer.start(CORRECTION_TIME);

		IOUtils.info("##################################");
		ec.createHistograms(inputPath1, inputPath2, nsplits);
		IOUtils.info("##################################");
		IOUtils.info("correcting errors in reads");
		List<Path> outputPaths = ec.correctErrors(IOUtils.getOutputPath1(), IOUtils.getOutputPath2());

		timer.stop(CORRECTION_TIME);

		IOUtils.info("############# OUTPUT #############");

		for(Path outputPath: outputPaths) {
			logger.debug("outputPath = {}", outputPath);

			String tempFileName = null;
			Path outputFile;

			if (outputPath.getParent().equals(IOUtils.getOutputPath1()))
				outputFile = new Path(ec.getOutputFile1()+"."+outputPath.getName());
			else
				outputFile = new Path(ec.getOutputFile2()+"."+outputPath.getName());

			logger.debug("outputFile = {}", outputFile);
			long outputFileCount = 0;

			if (fs.exists(outputPath)) {
				if (fs.isDirectory(outputPath)) {
					Path success = new Path(outputPath+Configuration.SLASH+"_SUCCESS");

					// Cleanup
					if (fs.exists(success))
						fs.delete(success, true);

					for (FileStatus file: Arrays.asList(fs.listStatus(outputPath))) {
						if (file.isFile() && file.getLen() > 0)
							outputFileCount++;
					}
					logger.debug("outputFileCount = {}", outputFileCount);
				}
			}

			if (outputFileCount == 1) {
				if (EXECUTION_ENGINE == ExecutionEngine.FLINK_MODE)
					tempFileName = outputPath+Configuration.SLASH+"1";
				else
					tempFileName = outputPath+Configuration.SLASH+fs.listStatus(outputPath)[0].getPath().getName();

				logger.debug("tempFileName = {}", tempFileName);
				fs.rename(new Path(tempFileName), outputFile);
				options.setMerge(false);
				IOUtils.info("output path = "+outputFile);

				if(config.HDFS_DELETE_TEMP_FILES)
					fs.delete(outputPath, true);
			} else if (options.getMerge()) {
				if (!options.runMergerThread()) {
					timer.start(MERGE_TIME);
					ec.mergeOutput(outputPath, outputFile);
					timer.stop(MERGE_TIME);
				}
				IOUtils.info("output path = "+outputFile);
			} else {
				IOUtils.info("output path = "+outputPath);
			}
		}

		IOUtils.info("##################################");

		timer.stop(TOTAL_TIME);

		/*
		 * Print times
		 */
		IOUtils.info("############# TIMES ##############");
		IOUtils.info("total runtime = "+IOUtils.formatTwoDecimal(timer.getTotalTime(TOTAL_TIME))+" seconds");
		IOUtils.info("  -initialization = "+IOUtils.formatTwoDecimal(timer.getTotalTime(INIT_TIME))+" seconds");
		IOUtils.info("  -error correction = "+IOUtils.formatTwoDecimal(timer.getTotalTime(CORRECTION_TIME))+" seconds");
		if (!options.runMergerThread() && options.getMerge())
			IOUtils.info("  -merge = "+IOUtils.formatTwoDecimal(timer.getTotalTime(MERGE_TIME))+" seconds");
		IOUtils.info("##################################");
		ec.printECTimes();

		System.exit(0);
	}
}