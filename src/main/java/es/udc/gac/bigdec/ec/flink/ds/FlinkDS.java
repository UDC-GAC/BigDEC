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
package es.udc.gac.bigdec.ec.flink.ds;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import es.udc.gac.bigdec.ec.CorrectionAlgorithm;
import es.udc.gac.bigdec.ec.ErrorCorrection;
import es.udc.gac.bigdec.ec.flink.CorrectPaired;
import es.udc.gac.bigdec.ec.flink.CorrectSingle;
import es.udc.gac.bigdec.ec.flink.FlinkEC;
import es.udc.gac.bigdec.ec.flink.HadoopFileInputFormat;
import es.udc.gac.bigdec.ec.flink.KmerGenPaired;
import es.udc.gac.bigdec.ec.flink.KmerGenSingle;
import es.udc.gac.bigdec.ec.flink.KmerHistogram;
import es.udc.gac.bigdec.ec.flink.QsHistogramPaired;
import es.udc.gac.bigdec.ec.flink.QsHistogramSingle;
import es.udc.gac.bigdec.ec.flink.RangePartitioner;
import es.udc.gac.bigdec.ec.flink.TextOuputFormat;
import es.udc.gac.bigdec.kmer.Kmer;
import es.udc.gac.bigdec.sequence.Sequence;
import es.udc.gac.bigdec.sequence.SequenceParser;
import es.udc.gac.bigdec.sequence.SequenceParserFactory;
import es.udc.gac.bigdec.util.CLIOptions;
import es.udc.gac.bigdec.util.Configuration;
import es.udc.gac.bigdec.util.IOUtils;
import es.udc.gac.hadoop.sequence.parser.mapreduce.PairText;
import es.udc.gac.hadoop.sequence.parser.mapreduce.PairedEndSequenceInputFormat;
import es.udc.gac.hadoop.sequence.parser.mapreduce.SingleEndSequenceInputFormat;

public class FlinkDS extends FlinkEC {

	private ExecutionEnvironment flinkExecEnv;
	private Job hadoopJob;
	private DataSet<Tuple2<LongWritable,Sequence>> readsDS;
	private DataSet<Tuple3<LongWritable,Sequence,Sequence>> pairedReadsDS;
	private DataSet<Tuple2<Kmer,Integer>> kmersDS;
	private RangePartitioner partitioner;

	public FlinkDS(Configuration config, CLIOptions options) {
		super(config, options);

		// Get Flink execution environment
		flinkExecEnv = ExecutionEnvironment.getExecutionEnvironment();

		if(config.FLINK_OBJECT_REUSE)
			flinkExecEnv.getConfig().enableObjectReuse();
		else
			flinkExecEnv.getConfig().disableObjectReuse();

		try {
			hadoopJob = Job.getInstance();
		} catch (IOException e) {
			IOUtils.error(e.getMessage());
		}

		// Set default parallelism and Hadoop config
		setParallelism(flinkExecEnv.getParallelism());
		setHadoopConfig(hadoopJob.getConfiguration());
	}

	@Override
	public long getEstimatedPartitionSize() {
		return partitioner.getPartitionSize();
	}

	@Override
	protected void createDatasets() throws IOException {
		// Create sequence parser
		SequenceParser parser = SequenceParserFactory.createParser(getFileFormat());

		long inputPath1Length = FileSystem.get(getHadoopConfig()).getFileStatus(getInputFile1()).getLen();

		// Create Range partitioner
		partitioner = new RangePartitioner(inputPath1Length, getParallelism());

		getLogger().info("Partitioning info: inputSize {}, partitions {}, partitionSize {}", inputPath1Length,
				getParallelism(), partitioner.getPartitionSize());

		if (!isPaired()) {
			SingleEndSequenceInputFormat inputFormat = IOUtils.getInputFormatInstance(getInputFormatClass());
			SingleEndSequenceInputFormat.setInputPaths(hadoopJob, getInputFile1());
			HadoopFileInputFormat<LongWritable,Text> hadoopIF = new HadoopFileInputFormat<LongWritable,Text>(inputFormat, LongWritable.class, Text.class, hadoopJob);
			hadoopIF.setAvgRecordBytes(getSequenceSize());

			DataSet<Tuple2<LongWritable,Text>> inputDS = flinkExecEnv.createInput(hadoopIF);

			readsDS = FlinkEC.parseSingleDS(inputDS, parser);
		} else {
			// Set left and right input paths for HSP
			PairedEndSequenceInputFormat.setLeftInputPath(getHadoopConfig(), getInputFile1(), getInputFormatClass());
			PairedEndSequenceInputFormat.setRightInputPath(getHadoopConfig(), getInputFile2(), getInputFormatClass());
			HadoopFileInputFormat<LongWritable,PairText> hadoopIF = new HadoopFileInputFormat<LongWritable,PairText>(new PairedEndSequenceInputFormat(), LongWritable.class, PairText.class, hadoopJob);
			hadoopIF.setAvgRecordBytes(getSequenceSize());

			DataSet<Tuple2<LongWritable,PairText>> inputDS = flinkExecEnv.createInput(hadoopIF);

			pairedReadsDS = FlinkEC.parsePairedDS(inputDS, parser);
		}
	}

	@Override
	protected int[] buildQsHistogram() throws IOException {
		if (!isPaired())
			readsDS.map(new QsHistogramSingle(FlinkEC.qsHistogram)).output(new DiscardingOutputFormat<>());
		else
			pairedReadsDS.map(new QsHistogramPaired(FlinkEC.qsHistogram)).output(new DiscardingOutputFormat<>());

		return null;
	}

	@Override
	protected void kmerCounting(short minKmerCounter, short maxKmerCounter) {
		if (!isPaired()) {
			kmersDS = readsDS.flatMap(new KmerGenSingle(getKmerLength(), isIgnoreNBases()));
		} else {
			kmersDS = pairedReadsDS.flatMap(new KmerGenPaired(getKmerLength(), isIgnoreNBases()));
		}

		kmersDS = kmersDS.groupBy(0).sum(1).filter(new FilterFunction<Tuple2<Kmer,Integer>>() {
			private static final long serialVersionUID = 2720097123780600026L;

			@Override
			public boolean filter(Tuple2<Kmer,Integer> kmer) {
				return kmer.f1 >= minKmerCounter;
			}
		}).withForwardedFields("f0;f1");
	}

	@Override
	protected int[] buildKmerHistrogram() throws IOException {
		JobExecutionResult result = null;
		TreeMap<Integer, Integer> treeMap = null;
		int[] kmerHistogram = new int[ErrorCorrection.KMER_HISTOGRAM_SIZE];

		kmersDS = kmersDS.map(new KmerHistogram(ErrorCorrection.KMER_HISTOGRAM_SIZE, FlinkEC.kmerHistogram));

		if (!getConfig().FLINK_WRITE_KMERS)
			kmersDS.output(new DiscardingOutputFormat<>());
		else
			kmersDS.writeAsCsv(getKmersPath().toString());

		try {
			result = flinkExecEnv.execute();
		} catch (Exception e) {
			IOUtils.error(e.getMessage());
		}

		treeMap = result.getAccumulatorResult(FlinkEC.kmerHistogram);

		for (Map.Entry<Integer,Integer> entry : treeMap.entrySet())
			kmerHistogram[entry.getKey()] = entry.getValue();

		treeMap = result.getAccumulatorResult(FlinkEC.qsHistogram);

		if (treeMap != null) {
			int[] qsHistogram = new int[ErrorCorrection.QS_HISTOGRAM_SIZE];

			for (Map.Entry<Integer,Integer> entry : treeMap.entrySet())
				qsHistogram[entry.getKey()] = entry.getValue();

			setQsHistogram(qsHistogram);
		}

		return kmerHistogram;
	}

	@Override
	protected void writeSolidKmersAsCSV(short kmerThreshold, short maxKmerCounter) {
		DataSet<Tuple2<Long,Integer>> kmers;

		if (!getConfig().FLINK_WRITE_KMERS) {
			kmerCounting(kmerThreshold, maxKmerCounter);
			kmersDS.writeAsCsv(getSolidKmersPath().toString());
		} else {
			kmers = flinkExecEnv.readCsvFile(getKmersPath().toString())
					.types(Long.class, Integer.class)
					.filter(new FilterFunction<Tuple2<Long,Integer>>() {
						private static final long serialVersionUID = -4321647988640959250L;

						@Override
						public boolean filter(Tuple2<Long,Integer> kmer) {
							return kmer.f1 >= kmerThreshold;
						}
					}).withForwardedFields("f0;f1");

			kmers.writeAsCsv(getSolidKmersPath().toString());			
		}

		try {
			flinkExecEnv.execute();
		} catch (Exception e) {
			IOUtils.error(e.getMessage());
		}

		kmers = null;
		kmersDS = null;
	}

	@Override
	protected void loadSolidKmers(int numberOfSolidKmers) {
		throw new RuntimeException("Not implemented");
	}

	@Override
	protected void filterSolidKmers(short kmerThreshold) {
		throw new RuntimeException("Not implemented");
	}

	@Override
	protected void removeSolidKmers() {}

	@Override
	protected void runErrorCorrection(CorrectionAlgorithm algorithm) {
		if (!isPaired()) {
			putMergePath(algorithm.getOutputPath1());
			correctSingleDataset(algorithm, getSolidKmersFile());
		} else {
			putMergePath(algorithm.getOutputPath1());
			putMergePath(algorithm.getOutputPath2());
			correctPairedDataset(algorithm, getSolidKmersFile());
		}

		try {
			flinkExecEnv.execute();
		} catch (Exception e) {
			IOUtils.error(e.getMessage());
		}
	}

	@Override
	protected void runErrorCorrection(List<CorrectionAlgorithm> correctionAlgorithms) {
		/*
		 *  For each correction algorithm
		 */
		for (CorrectionAlgorithm algorithm: correctionAlgorithms) {
			IOUtils.info("executing algorithm "+algorithm.toString());
			algorithm.printConfig();

			if (!isPaired())
				correctSingleDataset(algorithm, getSolidKmersFile());
			else
				correctPairedDataset(algorithm, getSolidKmersFile());
		}

		try {
			flinkExecEnv.execute();
		} catch (Exception e) {
			IOUtils.error(e.getMessage());
		}
	}

	@Override
	protected void destroyDatasets() {
		readsDS = null;
		pairedReadsDS = null;
	}

	private void correctSingleDataset(CorrectionAlgorithm algorithm, Path kmersFile) {
		org.apache.flink.core.fs.Path path = new org.apache.flink.core.fs.Path(algorithm.getOutputPath1().toString());
		TextOuputFormat<Sequence> tof = new TextOuputFormat<Sequence>(path, getHadoopConfig());

		// Correct and write reads
		if (getConfig().KEEP_ORDER) {
			getLogger().info("Range-Partitioner and sortPartition");

			readsDS.map(new CorrectSingle(algorithm, true, kmersFile.toString(), KMER_MAX_COUNTER))
			.partitionCustom(partitioner, 0).sortPartition(0, Order.ASCENDING)
			.map(read -> read.f1).withForwardedFields("f1.*->*").output(tof);
		} else {
			if (getCLIOptions().runMergerThread()) {
				getLogger().info("Range-Partitioner");

				readsDS.map(new CorrectSingle(algorithm, true, kmersFile.toString(), KMER_MAX_COUNTER))
				.partitionCustom(partitioner, 0)
				.map(read -> read.f1).withForwardedFields("f1.*->*").output(tof);
			} else {
				readsDS.map(new CorrectSingle(algorithm, true, kmersFile.toString(), KMER_MAX_COUNTER))
				.map(read -> read.f1).withForwardedFields("f1.*->*").output(tof);
			}
		}
	}

	private void correctPairedDataset(CorrectionAlgorithm algorithm, Path kmersFile) {
		org.apache.flink.core.fs.Path path1 = new org.apache.flink.core.fs.Path(algorithm.getOutputPath1().toString());
		org.apache.flink.core.fs.Path path2 = new org.apache.flink.core.fs.Path(algorithm.getOutputPath2().toString());
		TextOuputFormat<Sequence> tof1 = new TextOuputFormat<Sequence>(path1, getHadoopConfig());
		TextOuputFormat<Sequence> tof2 = new TextOuputFormat<Sequence>(path2, getHadoopConfig());
		DataSet<Tuple3<LongWritable,Sequence,Sequence>> corrReadsDS;

		// Correct and write reads
		if (getConfig().KEEP_ORDER) {
			getLogger().info("Range-Partitioner and sortPartition");

			corrReadsDS = pairedReadsDS.map(new CorrectPaired(algorithm, true, kmersFile.toString(), KMER_MAX_COUNTER))
					.partitionCustom(partitioner, 0).sortPartition(0, Order.ASCENDING);
		} else {
			if (getCLIOptions().runMergerThread()) {
				getLogger().info("Range-Partitioner");

				corrReadsDS = pairedReadsDS.map(new CorrectPaired(algorithm, true, kmersFile.toString(), KMER_MAX_COUNTER))
						.partitionCustom(partitioner, 0);
			} else {
				corrReadsDS = pairedReadsDS.map(new CorrectPaired(algorithm, true, kmersFile.toString(), KMER_MAX_COUNTER));
			}
		}

		corrReadsDS.map(read -> read.f1).withForwardedFields("f1.*->*").output(tof1);
		corrReadsDS.map(read -> read.f2).withForwardedFields("f2.*->*").output(tof2);
	}
}
