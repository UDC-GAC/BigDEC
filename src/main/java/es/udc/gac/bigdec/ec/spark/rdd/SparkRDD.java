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
package es.udc.gac.bigdec.ec.spark.rdd;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;

import es.udc.gac.bigdec.ec.ErrorCorrection;
import es.udc.gac.bigdec.ec.spark.SparkEC;
import es.udc.gac.bigdec.kmer.KmerKey;
import es.udc.gac.bigdec.ec.spark.KmerMap;
import es.udc.gac.bigdec.ec.CorrectionAlgorithm;
import es.udc.gac.bigdec.sequence.Sequence;
import es.udc.gac.bigdec.sequence.SequenceParser;
import es.udc.gac.bigdec.sequence.SequenceParserFactory;
import es.udc.gac.bigdec.util.CLIOptions;
import es.udc.gac.bigdec.util.Configuration;
import es.udc.gac.hadoop.sequence.parser.mapreduce.PairText;
import es.udc.gac.hadoop.sequence.parser.mapreduce.PairedEndSequenceInputFormat;

public class SparkRDD extends SparkEC {

	private JavaRDD<Sequence> readsRDD;
	private JavaPairRDD<Sequence,Sequence> pairedReadsRDD;
	private JavaPairRDD<KmerKey,Short> kmersRDD;
	private JavaRDD<Integer> dummyIntegerRDD;

	public SparkRDD(Configuration config, CLIOptions options) {
		super(config, options);
	}

	@Override
	protected void createDatasets() throws IOException {
		// Create sequence parser
		SequenceParser parser = SequenceParserFactory.createParser(getFileFormat());
		Broadcast<SequenceParser> parserBC = getjSparkContext().broadcast(parser);

		if (!isPaired()) {
			JavaPairRDD<LongWritable,Text> inputReadsRDD = getjSparkContext().newAPIHadoopFile(getInputFile1().toString(), 
					getInputFormatClass(), LongWritable.class, Text.class, getHadoopConfig());

			readsRDD = SparkEC.parseSingleRDD(inputReadsRDD, parserBC);
			inputReadsRDD = null;

			getLogger().info("JavaRDD<Sequence> readsRDD: npartitions {}, partitioner {}", readsRDD.getNumPartitions(), readsRDD.partitioner());

			if (getConfig().SPARK_SERIALIZE_RDD)
				readsRDD.persist(StorageLevel.MEMORY_ONLY_SER());
			else
				readsRDD.persist(StorageLevel.MEMORY_ONLY());
		} else {
			// Set left and right input paths for HSP
			PairedEndSequenceInputFormat.setLeftInputPath(getHadoopConfig(), getInputFile1(), getInputFormatClass());
			PairedEndSequenceInputFormat.setRightInputPath(getHadoopConfig(), getInputFile2(), getInputFormatClass());

			JavaPairRDD<LongWritable,PairText> inputReadsRDD = getjSparkContext().newAPIHadoopFile(getInputFile1().toString(), 
					PairedEndSequenceInputFormat.class, LongWritable.class, PairText.class, getHadoopConfig());

			pairedReadsRDD = SparkEC.parsePairedRDD(inputReadsRDD, parserBC);
			inputReadsRDD = null;

			getLogger().info("JavaPairRDD<Sequence,Sequence> pairedReadsRDD: npartitions {}, partitioner {}", pairedReadsRDD.getNumPartitions(), pairedReadsRDD.partitioner());

			if (getConfig().SPARK_SERIALIZE_RDD)
				pairedReadsRDD.persist(StorageLevel.MEMORY_ONLY_SER());
			else
				pairedReadsRDD.persist(StorageLevel.MEMORY_ONLY());
		}
	}

	@Override
	protected int[] buildQsHistogram() {
		int[] qsHistogram = null;

		if (!isPaired()) {
			qsHistogram = readsRDD.mapPartitions(new QsHistogramSingle(ErrorCorrection.QS_HISTOGRAM_SIZE))
					.reduce((x, y) -> {
						for (int i=0;i<x.length;i++) x[i]+=y[i];
						return x;
					});
		} else {
			qsHistogram = pairedReadsRDD.mapPartitions(new QsHistogramPaired(ErrorCorrection.QS_HISTOGRAM_SIZE))
					.reduce((x, y) -> {
						for (int i=0;i<x.length;i++)
							x[i]+=y[i];
						return x;
					});
		}

		return qsHistogram;
	}

	@Override
	protected void kmerCounting(short minKmerCounter, short maxKmerCounter) {
		Broadcast<Short> minCounterBC = getjSparkContext().broadcast(minKmerCounter);
		Broadcast<Short> maxCounterBC = getjSparkContext().broadcast(maxKmerCounter);

		if (!isPaired()) {
			kmersRDD = readsRDD.flatMapToPair(new KmerGenSingle(getKmerLength(), isIgnoreNBases()))
					.reduceByKey((x, y) -> (short) ((x + y > maxCounterBC.value())? maxCounterBC.value() : x + y), getShufflePartitions())
					.filter(kmer -> kmer._2 >= minCounterBC.value());
		} else {
			kmersRDD = pairedReadsRDD.flatMapToPair(new KmerGenPaired(getKmerLength(), isIgnoreNBases()))
					.reduceByKey((x, y) -> (short) ((x + y > maxCounterBC.value())? maxCounterBC.value() : x + y), getShufflePartitions())
					.filter(kmer -> kmer._2 >= minCounterBC.value());
		}

		if (getConfig().SPARK_SERIALIZE_RDD)
			kmersRDD.persist(StorageLevel.MEMORY_ONLY_SER());
		else
			kmersRDD.persist(StorageLevel.MEMORY_ONLY());

		getLogger().info("JavaPairRDD<Kmer,Short> kmersRDD: npartitions {}, partitioner {}", kmersRDD.getNumPartitions(), kmersRDD.partitioner());
	}

	@Override
	protected int[] buildKmerHistrogram() {
		return kmersRDD.mapPartitions(new KmerHistogram(ErrorCorrection.KMER_HISTOGRAM_SIZE))
				.reduce((x, y) -> {
					for (int i=0;i<x.length;i++)
						x[i]+=y[i];
					return x;
				});
	}

	@Override
	protected void writeSolidKmersAsCSV(short kmerThreshold, short maxKmerCounter) {
		Broadcast<Short> kmerThresholdBC = getjSparkContext().broadcast(kmerThreshold);

		// Filter and write k-mers
		kmersRDD.filter(kmer -> kmer._2 >= kmerThresholdBC.value()).map(kmer -> {
			return kmer._1.toString()+","+kmer._2.toString();
		}).saveAsTextFile(getSolidKmersPath().toString());

		kmersRDD.unpersist(false);
		kmersRDD = null;
		kmerThresholdBC.destroy(false);
	}

	@Override
	protected void loadSolidKmers(int numberOfSolidKmers) {
		dummyIntegerRDD = getjSparkContext().parallelize(IntStream.range(0, getParallelism()).boxed().collect(Collectors.toList()),
				getParallelism());

		Broadcast<Integer> numberOfSolidKmersBC = getjSparkContext().broadcast(numberOfSolidKmers);
		Broadcast<String> kmersFileBC = getjSparkContext().broadcast(getSolidKmersFile().toString());

		dummyIntegerRDD.foreachPartition(iter -> {
			if (iter.hasNext())
				KmerMap.load(kmersFileBC.value(), numberOfSolidKmersBC.value());
		});

		numberOfSolidKmersBC.destroy(false);
		kmersFileBC.destroy(false);
	}

	@Override
	protected void filterSolidKmers(short kmerOccurrenceThreshold) {
		Broadcast<Short> kmerThresholdBC = getjSparkContext().broadcast(kmerOccurrenceThreshold);

		dummyIntegerRDD.foreachPartition(iter -> {
			if (iter.hasNext())
				KmerMap.filter(kmerThresholdBC.value());
		});

		kmerThresholdBC.destroy(false);
	}

	@Override
	protected void removeSolidKmers() {
		dummyIntegerRDD.foreachPartition(iter -> {
			if (iter.hasNext())
				KmerMap.remove();
		});
	}

	@Override
	protected void runErrorCorrection(CorrectionAlgorithm algorithm) {
		// Broadcast correction algorithm
		Broadcast<CorrectionAlgorithm> correctionAlgorithmBC = getjSparkContext().broadcast(algorithm);

		if (!isPaired()) {
			putMergePath(algorithm.getOutputPath1());
			correctDataset(readsRDD, correctionAlgorithmBC, algorithm.getOutputPath1());
		} else {
			putMergePath(algorithm.getOutputPath1());
			correctDataset(pairedReadsRDD.keys(), correctionAlgorithmBC, algorithm.getOutputPath1());
			putMergePath(algorithm.getOutputPath2());
			correctDataset(pairedReadsRDD.values(), correctionAlgorithmBC, algorithm.getOutputPath2());
		}

		correctionAlgorithmBC.destroy(false);
	}

	@Override
	protected void runErrorCorrection(List<CorrectionAlgorithm> correctionAlgorithms) {
		throw new RuntimeException("Not implemented");
	}

	@Override
	protected void destroyDatasets() {
		if (!isPaired())
			readsRDD = null;
		else
			pairedReadsRDD = null;

		dummyIntegerRDD = null;
		getSparkSession().stop();
	}

	private static void correctDataset(JavaRDD<Sequence> readsRDD, Broadcast<CorrectionAlgorithm> correctionAlgorithmBC, Path outputPath) {
		// Correct and write reads
		readsRDD.map(sequence -> {
			correctionAlgorithmBC.value().setSolidKmers(KmerMap.getSolidKmers());
			return correctionAlgorithmBC.value().correctRead(sequence);
		}).saveAsTextFile(outputPath.toString());
	}
}
