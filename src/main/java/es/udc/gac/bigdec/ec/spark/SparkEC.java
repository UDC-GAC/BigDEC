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
package es.udc.gac.bigdec.ec.spark;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import es.udc.gac.bigdec.RunEC;
import es.udc.gac.bigdec.ec.ErrorCorrection;
import es.udc.gac.bigdec.sequence.Sequence;
import es.udc.gac.bigdec.sequence.SequenceParser;
import es.udc.gac.bigdec.util.CLIOptions;
import es.udc.gac.bigdec.util.Configuration;
import es.udc.gac.bigdec.util.IOUtils;
import es.udc.gac.hadoop.sequence.parser.mapreduce.PairText;
import scala.Tuple2;

public abstract class SparkEC extends ErrorCorrection {

	private SparkSession sparkSession;
	private JavaSparkContext jSparkContext;
	private int shufflePartitions;

	public SparkEC(Configuration config, CLIOptions options) {
		super(config, options);

		// Create Spark session and context
		sparkSession = SparkSession.builder().config(createSparkConf()).getOrCreate();
		jSparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

		// Set default parallelism, shuffle partitions and Hadoop config
		setParallelism(sparkSession.sparkContext().defaultParallelism());
		setHadoopConfig(jSparkContext.hadoopConfiguration());
		shufflePartitions = getParallelism() * config.SPARK_SHUFFLE_PARTITIONS;

		getLogger().info("shufflePartitions = {}", shufflePartitions);
	}

	public SparkSession getSparkSession() {
		return sparkSession;
	}

	public JavaSparkContext getjSparkContext() {
		return jSparkContext;
	}

	public int getShufflePartitions() {
		return shufflePartitions;
	}

	public static JavaRDD<Sequence> parseSingleRDD(JavaPairRDD<LongWritable,Text> inputReadsRDD, Broadcast<SequenceParser> parserBC) {

		return inputReadsRDD.values().map(new Function<Text, Sequence>() {
			private static final long serialVersionUID = 1647254546130571830L;

			@Override
			public Sequence call(Text read) {
				return parserBC.value().parseSequence(read.getBytes(), read.getLength());
			}
		});
	}

	public static JavaPairRDD<Sequence,Sequence> parsePairedRDD(JavaPairRDD<LongWritable,PairText> inputReadsRDD, Broadcast<SequenceParser> parserBC) {

		return inputReadsRDD.values().mapToPair(new PairFunction<PairText, Sequence, Sequence>() {
			private static final long serialVersionUID = 1647254546130571830L;

			@Override
			public Tuple2<Sequence,Sequence> call(PairText read) {
				Sequence left = parserBC.value().parseSequence(read.getLeft().getBytes(), read.getLeft().getLength());
				Sequence right = parserBC.value().parseSequence(read.getRight().getBytes(), read.getRight().getLength());
				return new Tuple2<Sequence,Sequence>(left, right);
			}
		});
	}

	private SparkConf createSparkConf() {
		SparkConf sparkConf = new SparkConf()
				.setAppName(RunEC.APP_NAME)
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				.set("spark.kryo.registrationRequired", "false")
				.set("spark.kryo.unsafe", "true")
				.set("spark.kryo.referenceTracking", "false")
				.set("spark.kryoserializer.buffer", "1m")
				.set("spark.broadcast.checksum", "false")
				.set("spark.shuffle.service.enabled", "false")
				.set("spark.shuffle.compress", "true")
				.set("spark.shuffle.spill.compress", "true")
				.set("spark.checkpoint.compress", "true")
				.set("spark.broadcast.compress", "true")
				.set("spark.rdd.compress", String.valueOf(getConfig().SPARK_COMPRESS_DATA))
				.set("spark.io.compression.codec", String.valueOf(getConfig().SPARK_COMPRESSION_CODEC))
				.set("spark.speculation", "false")
				.set("spark.dynamicAllocation.enabled", "false")
				.set("spark.driver.maxResultSize", "0")
				.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
				.set("spark.hadoop.dfs.replication", String.valueOf(getConfig().HDFS_BLOCK_REPLICATION));

		// Register custom classes with Kryo
		try {
			sparkConf.registerKryoClasses(new Class<?>[]{
				Class.forName("es.udc.gac.bigdec.sequence.Sequence"),
				Class.forName("es.udc.gac.bigdec.sequence.SequenceParser"),
				Class.forName("es.udc.gac.bigdec.sequence.FastQParser"),
				Class.forName("es.udc.gac.bigdec.sequence.SequenceParserFactory"),
				Class.forName("es.udc.gac.bigdec.sequence.SequenceParserFactory$FileFormat"),
				Class.forName("es.udc.gac.bigdec.kmer.Kmer"),
				Class.forName("es.udc.gac.bigdec.kmer.KmerKey"),
				Class.forName("es.udc.gac.bigdec.kmer.KmerGenerator"),
				Class.forName("es.udc.gac.bigdec.util.CLIOptions"),
				Class.forName("es.udc.gac.bigdec.util.Configuration"),
				Class.forName("es.udc.gac.bigdec.util.IOUtils"),
				Class.forName("es.udc.gac.bigdec.util.MurmurHash3"),
				Class.forName("es.udc.gac.bigdec.util.Timer"),
				Class.forName("es.udc.gac.bigdec.RunEC"),
				Class.forName("es.udc.gac.bigdec.RunMerge"),
				Class.forName("es.udc.gac.bigdec.ec.CorrectionAlgorithm"),
				Class.forName("es.udc.gac.bigdec.ec.CorrectionAlgorithmException"),
				Class.forName("es.udc.gac.bigdec.ec.Correction"),
				Class.forName("es.udc.gac.bigdec.ec.ErrorCorrection"),
				Class.forName("es.udc.gac.bigdec.ec.MergerThread"),
				Class.forName("es.udc.gac.bigdec.ec.SolidRegion"),
				Class.forName("es.udc.gac.bigdec.ec.SolidRegionComp"),
				Class.forName("es.udc.gac.bigdec.ec.bless2.BLESS2"),
				Class.forName("es.udc.gac.bigdec.ec.bless2.CandidatePath"),
				Class.forName("es.udc.gac.bigdec.ec.bless2.Trimming"),
				Class.forName("es.udc.gac.bigdec.ec.reckoner.RECKONER"),
				Class.forName("es.udc.gac.bigdec.ec.reckoner.CandidatePath"),
				Class.forName("es.udc.gac.bigdec.ec.musket.MUSKET"),
				Class.forName("es.udc.gac.bigdec.ec.spark.SparkEC"),
				Class.forName("es.udc.gac.bigdec.ec.spark.ds.SparkDS"),
				Class.forName("es.udc.gac.bigdec.ec.spark.ds.KmerGenSingle"),
				Class.forName("es.udc.gac.bigdec.ec.spark.ds.KmerGenPaired"),
				Class.forName("es.udc.gac.bigdec.ec.spark.ds.QsHistogramSingle"),
				Class.forName("es.udc.gac.bigdec.ec.spark.ds.QsHistogramPaired"),
				Class.forName("es.udc.gac.bigdec.ec.spark.rdd.SparkRDD"),
				Class.forName("es.udc.gac.bigdec.ec.spark.rdd.KmerGenSingle"),
				Class.forName("es.udc.gac.bigdec.ec.spark.rdd.KmerGenPaired"),
				Class.forName("es.udc.gac.bigdec.ec.spark.rdd.QsHistogramSingle"),
				Class.forName("es.udc.gac.bigdec.ec.spark.rdd.QsHistogramPaired"),
				Class.forName("java.util.Map"),
				Class.forName("java.util.HashMap"),
				Class.forName("java.util.List"),
				Class.forName("java.util.ArrayList"),
				Class.forName("java.lang.Boolean"),
				Class.forName("java.lang.Integer"),
				Class.forName("java.lang.Long"),
				Class.forName("java.lang.Double"),
				Class.forName("java.lang.Short"),
				Class.forName("java.lang.Byte"),
				Class.forName("java.lang.String"),
				Class.forName("java.lang.StringBuilder"),
				Class.forName("scala.Tuple2"),
				Class.forName("org.apache.hadoop.io.Text"),
				Class.forName("org.apache.hadoop.io.LongWritable"),
				Class.forName("es.udc.gac.hadoop.sequence.parser.mapreduce.PairText")
			});
		} catch (ClassNotFoundException e) {
			IOUtils.error("Kryo register class: "+e.getMessage());
		}

		return sparkConf;
	}
}
