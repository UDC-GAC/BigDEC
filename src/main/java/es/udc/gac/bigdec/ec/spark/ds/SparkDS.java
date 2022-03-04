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
package es.udc.gac.bigdec.ec.spark.ds;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.storage.StorageLevel;

import es.udc.gac.bigdec.ec.ErrorCorrection;
import es.udc.gac.bigdec.ec.spark.SparkEC;
import es.udc.gac.bigdec.kmer.Kmer;
import es.udc.gac.bigdec.ec.spark.KmerMap;
import es.udc.gac.bigdec.ec.CorrectionAlgorithm;
import es.udc.gac.bigdec.sequence.Sequence;
import es.udc.gac.bigdec.sequence.SequenceParser;
import es.udc.gac.bigdec.sequence.SequenceParserFactory;
import es.udc.gac.bigdec.util.CLIOptions;
import es.udc.gac.bigdec.util.Configuration;
import es.udc.gac.hadoop.sequence.parser.mapreduce.PairText;
import es.udc.gac.hadoop.sequence.parser.mapreduce.PairedEndSequenceInputFormat;
import scala.Tuple2;

public class SparkDS extends SparkEC {

	private Dataset<Sequence> readsDS;
	private Dataset<Tuple2<Sequence,Sequence>> pairedReadsDS;
	private Dataset<Tuple2<Kmer,Object>> kmersDS;
	private Dataset<Integer> dummyIntegerDS;
	private Encoder<int[]> intArrayEncoder;
	private Encoder<Kmer> encoderKmer;
	private Encoder<Tuple2<Sequence,Sequence>> encoderSequenceTuple2;
	private Encoder<Tuple2<Kmer,Short>> encoderKmerTuple2;

	public SparkDS(Configuration config, CLIOptions options) {
		super(config, options);

		intArrayEncoder = getSparkSession().implicits().newIntArrayEncoder();
		encoderSequenceTuple2 = Encoders.tuple(Encoders.bean(Sequence.class), Encoders.bean(Sequence.class));
		encoderKmer = Encoders.bean(Kmer.class);
		encoderKmerTuple2 = Encoders.tuple(encoderKmer, Encoders.SHORT());

		// Set SQL shuffle partitions
		getSparkSession().sqlContext().setConf("spark.sql.shuffle.partitions", String.valueOf(getShufflePartitions()));
	}

	@Override
	protected void createDatasets() throws IOException {
		// Create sequence parser
		SequenceParser parser = SequenceParserFactory.createParser(getFileFormat());
		Broadcast<SequenceParser> parserBC = getjSparkContext().broadcast(parser);

		if (!isPaired()) {
			JavaPairRDD<LongWritable,Text> inputReadsRDD = getjSparkContext().newAPIHadoopFile(getInputFile1().toString(), 
					getInputFormatClass(), LongWritable.class, Text.class, getHadoopConfig());

			JavaRDD<Sequence> readsRDD = SparkEC.parseSingleRDD(inputReadsRDD, parserBC);
			inputReadsRDD = null;

			readsDS = getSparkSession().createDataset(readsRDD.rdd(), Encoders.bean(Sequence.class));

			getLogger().info("JavaRDD<Sequence> readsRDD: npartitions {}, partitioner {}", readsRDD.getNumPartitions(), readsRDD.partitioner());
			getLogger().info("Dataset<Sequence> readsDS: npartitions {}, partitioner {}", readsDS.rdd().getNumPartitions(), readsDS.rdd().partitioner());

			readsDS.persist(StorageLevel.MEMORY_ONLY());
		} else {
			// Set left and right input paths for HSP
			PairedEndSequenceInputFormat.setLeftInputPath(getHadoopConfig(), getInputFile1(), getInputFormatClass());
			PairedEndSequenceInputFormat.setRightInputPath(getHadoopConfig(), getInputFile2(), getInputFormatClass());

			JavaPairRDD<LongWritable,PairText> inputReadsRDD = getjSparkContext().newAPIHadoopFile(getInputFile1().toString(), 
					PairedEndSequenceInputFormat.class, LongWritable.class, PairText.class, getHadoopConfig());

			JavaPairRDD<Sequence,Sequence> pairedReadsRDD = SparkEC.parsePairedRDD(inputReadsRDD, parserBC);
			inputReadsRDD = null;

			pairedReadsDS = getSparkSession().createDataset(pairedReadsRDD.rdd(), encoderSequenceTuple2);

			getLogger().info("JavaPairRDD<Sequence,Sequence> pairedReadsRDD: npartitions {}, partitioner {}", pairedReadsRDD.getNumPartitions(), pairedReadsRDD.partitioner());
			getLogger().info("Dataset<Tuple2<Sequence,Sequence>> pairedReadsDS: npartitions {}, partitioner {}", pairedReadsDS.rdd().getNumPartitions(), pairedReadsDS.rdd().partitioner());

			pairedReadsDS.persist(StorageLevel.MEMORY_ONLY());
		}
	}

	@Override
	protected int[] buildQsHistogram() {
		int[] qsHistogram = null;

		if (!isPaired()) {
			qsHistogram = readsDS.mapPartitions(new QsHistogramSingle(ErrorCorrection.QS_HISTOGRAM_SIZE), intArrayEncoder)
					.reduce((org.apache.spark.api.java.function.ReduceFunction<int[]>) (x, y) -> {
						for (int i=0;i<x.length;i++)
							x[i]+=y[i];
						return x;
					});
		} else {
			qsHistogram = pairedReadsDS.mapPartitions(new QsHistogramPaired(ErrorCorrection.QS_HISTOGRAM_SIZE), intArrayEncoder)
					.reduce((org.apache.spark.api.java.function.ReduceFunction<int[]>) (x, y) -> {
						for (int i=0;i<x.length;i++)
							x[i]+=y[i];
						return x;
					});
		}

		return qsHistogram;
	}

	@Override
	protected void kmerCounting(short minKmerCounter, short maxKmerCounter) {
		String filterCondition = "count(1) >= "+minKmerCounter;
		Broadcast<String> filterConditionBC = getjSparkContext().broadcast(filterCondition);

		if (!isPaired()) {
			kmersDS = readsDS.flatMap(new KmerGenSingle(getKmerLength(), isIgnoreNBases()), encoderKmerTuple2)
					.groupByKey((MapFunction<Tuple2<Kmer,Short>, Kmer>) x -> x._1, encoderKmer)
					.count()
					.filter(filterConditionBC.value());
		} else {
			kmersDS = pairedReadsDS.flatMap(new KmerGenPaired(getKmerLength(), isIgnoreNBases()), encoderKmerTuple2)
					.groupByKey((MapFunction<Tuple2<Kmer,Short>, Kmer>) x -> x._1, encoderKmer)
					.count()
					.filter(filterConditionBC.value());
		}

		kmersDS.persist(StorageLevel.MEMORY_ONLY());
	}

	protected int[] buildKmerHistrogram() {
		return kmersDS.mapPartitions(new KmerHistogram(ErrorCorrection.KMER_HISTOGRAM_SIZE), intArrayEncoder)
				.reduce((org.apache.spark.api.java.function.ReduceFunction<int[]>) (x, y) -> {
					for (int i=0;i<x.length;i++)
						x[i]+=y[i];
					return x;
				});
	}

	@Override
	protected void writeSolidKmersAsCSV(short kmerThreshold, short maxKmerCounter) {
		Broadcast<Short> kmerThresholdBC = getjSparkContext().broadcast(kmerThreshold);
		Broadcast<Short> maxKmerCounterBC = getjSparkContext().broadcast(maxKmerCounter);

		// Filter and write k-mers
		kmersDS.filter((org.apache.spark.api.java.function.FilterFunction<Tuple2<Kmer,Object>>)
				kmer -> ((Long)kmer._2) >= kmerThresholdBC.value()).map((MapFunction<Tuple2<Kmer,Object>,String>)
						kmer -> {
							if (((Long)kmer._2) >= maxKmerCounterBC.value())
								return kmer._1.toString()+","+maxKmerCounterBC.value();

							return kmer._1.toString()+","+((Long)kmer._2).toString();
						}, Encoders.STRING()).write().format("text").save(getSolidKmersPath().toString());

		kmersDS.unpersist(false);
		kmersDS = null;
		kmerThresholdBC.destroy(false);
		maxKmerCounterBC.destroy(false);
	}

	@Override
	protected void loadSolidKmers(int numberOfSolidKmers) {
		JavaRDD<Integer> dummyIntegerRDD = getjSparkContext().parallelize(IntStream.range(0, getParallelism()).boxed().collect(Collectors.toList()),
				getParallelism());

		dummyIntegerDS = getSparkSession().createDataset(dummyIntegerRDD.rdd(), Encoders.INT());

		Broadcast<Integer> numberOfSolidKmersBC = getjSparkContext().broadcast(numberOfSolidKmers);
		Broadcast<String> kmersFileBC = getjSparkContext().broadcast(getSolidKmersFile().toString());

		dummyIntegerDS.foreachPartition(iter -> {
			if (iter.hasNext())
				KmerMap.load(kmersFileBC.value(), numberOfSolidKmersBC.value());
		});

		numberOfSolidKmersBC.destroy(false);
		kmersFileBC.destroy(false);
	}

	@Override
	protected void filterSolidKmers(short kmerOccurrenceThreshold) {
		Broadcast<Short> kmerThresholdBC = getjSparkContext().broadcast(kmerOccurrenceThreshold);

		dummyIntegerDS.foreachPartition(iter -> {
			if (iter.hasNext())
				KmerMap.filter(kmerThresholdBC.value());
		});

		kmerThresholdBC.destroy(false);
	}

	@Override
	protected void removeSolidKmers() {
		dummyIntegerDS.foreachPartition(iter -> {
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
			correctDataset(readsDS, correctionAlgorithmBC, algorithm.getOutputPath1());
		} else {
			Dataset<Sequence> corrReadsDS;
			corrReadsDS = pairedReadsDS.map((MapFunction<Tuple2<Sequence,Sequence>,Sequence>) x -> x._1,
					Encoders.bean(Sequence.class));
			putMergePath(algorithm.getOutputPath1());
			correctDataset(corrReadsDS, correctionAlgorithmBC, algorithm.getOutputPath1());
			corrReadsDS = pairedReadsDS.map((MapFunction<Tuple2<Sequence,Sequence>,Sequence>) x -> x._2,
					Encoders.bean(Sequence.class));
			putMergePath(algorithm.getOutputPath2());
			correctDataset(corrReadsDS, correctionAlgorithmBC, algorithm.getOutputPath2());
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
			readsDS = null;
		else
			pairedReadsDS = null;

		dummyIntegerDS = null;
		getSparkSession().stop();
	}

	private static void correctDataset(Dataset<Sequence> readsDS, Broadcast<CorrectionAlgorithm> correctionAlgorithmBC, Path outputPath) {
		// Correct and write reads
		readsDS.map((MapFunction<Sequence,Sequence>) sequence -> {
			correctionAlgorithmBC.value().setSolidKmers(KmerMap.getSolidKmers());
			return correctionAlgorithmBC.value().correctRead(sequence);
		}, Encoders.bean(Sequence.class))
		.map((MapFunction<Sequence,String>) x -> x.toString(), Encoders.STRING())
		.write().format("text").save(outputPath.toString());
	}
}
