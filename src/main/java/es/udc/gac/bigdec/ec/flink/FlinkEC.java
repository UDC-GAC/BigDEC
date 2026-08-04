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
package es.udc.gac.bigdec.ec.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import es.udc.gac.bigdec.ec.ErrorCorrection;
import es.udc.gac.bigdec.sequence.Sequence;
import es.udc.gac.bigdec.sequence.SequenceParser;
import es.udc.gac.bigdec.util.CLIOptions;
import es.udc.gac.bigdec.util.Configuration;
import es.udc.gac.hadoop.sequence.parser.mapreduce.PairText;

public abstract class FlinkEC extends ErrorCorrection {

	protected static final String qsHistogram = "qsHistogram";
	protected static final String kmerHistogram = "kmerHistogram";

	public FlinkEC(Configuration config, CLIOptions options) {
		super(config, options);
	}

	public static DataSet<Tuple2<LongWritable,Sequence>> parseSingleDS(DataSet<Tuple2<LongWritable,Text>> inputDS,
			SequenceParser parser) {

		DataSet<Tuple2<LongWritable,Sequence>> inputReadsDS = 
				inputDS.map(new MapFunction<Tuple2<LongWritable,Text>, Tuple2<LongWritable,Sequence>>() {

					private static final long serialVersionUID = 1590147064464369799L;
					private final Tuple2<LongWritable,Sequence> tuple2 = new Tuple2<LongWritable,Sequence>();
					private final Sequence buffer = new Sequence();
					
					@Override
					public Tuple2<LongWritable,Sequence> map(Tuple2<LongWritable,Text> read) throws Exception {
						parser.parseSequence(read.f1.getBytes(), read.f1.getLength(), buffer);
						tuple2.setFields(read.f0, buffer);
						return tuple2;
					}
				});

		return inputReadsDS;
	}

	public static DataStream<Tuple2<LongWritable,Sequence>> parseSingleDS(DataStream<Tuple2<LongWritable,Text>> inputDS,
			SequenceParser parser) {

		DataStream<Tuple2<LongWritable,Sequence>> inputReadsDS = 
				inputDS.map(new MapFunction<Tuple2<LongWritable,Text>, Tuple2<LongWritable,Sequence>>() {

					private static final long serialVersionUID = 6859750261852235091L;
					private final Tuple2<LongWritable,Sequence> tuple2 = new Tuple2<LongWritable,Sequence>();
					private final Sequence buffer = new Sequence();
					
					@Override
					public Tuple2<LongWritable,Sequence> map(Tuple2<LongWritable,Text> read) throws Exception {
						parser.parseSequence(read.f1.getBytes(), read.f1.getLength(), buffer);
						tuple2.setFields(read.f0, buffer);
						return tuple2;
					}
				});

		return inputReadsDS;
	}

	public static DataSet<Tuple3<LongWritable,Sequence,Sequence>> parsePairedDS(DataSet<Tuple2<LongWritable,PairText>> inputDS,
			SequenceParser parser) {

		DataSet<Tuple3<LongWritable,Sequence,Sequence>> inputReadsDS = 
				inputDS.map(new MapFunction<Tuple2<LongWritable,PairText>, Tuple3<LongWritable,Sequence,Sequence>>() {

					private static final long serialVersionUID = 3540455096015091189L;
					private final Tuple3<LongWritable,Sequence,Sequence> tuple3 = new Tuple3<LongWritable,Sequence,Sequence>();
					private final Sequence leftBuffer = new Sequence();
					private final Sequence rightBuffer = new Sequence();
					
					@Override
					public Tuple3<LongWritable,Sequence,Sequence> map(Tuple2<LongWritable,PairText> read) throws Exception {
						parser.parseSequence(read.f1.getLeft().getBytes(), read.f1.getLeft().getLength(), leftBuffer);
						parser.parseSequence(read.f1.getRight().getBytes(), read.f1.getRight().getLength(), rightBuffer);
						tuple3.setFields(read.f0, leftBuffer, rightBuffer);
						return tuple3;
					}
				});

		return inputReadsDS;
	}

	public static DataStream<Tuple3<LongWritable,Sequence,Sequence>> parsePairedDS(DataStream<Tuple2<LongWritable,PairText>> inputDS,
			SequenceParser parser) {

		DataStream<Tuple3<LongWritable,Sequence,Sequence>> inputReadsDS = 
				inputDS.map(new MapFunction<Tuple2<LongWritable,PairText>, Tuple3<LongWritable,Sequence,Sequence>>() {

					private static final long serialVersionUID = -5523818680286523838L;
					private final Tuple3<LongWritable,Sequence,Sequence> tuple3 = new Tuple3<LongWritable,Sequence,Sequence>();
					private final Sequence leftBuffer = new Sequence();
					private final Sequence rightBuffer = new Sequence();
					
					@Override
					public Tuple3<LongWritable,Sequence,Sequence> map(Tuple2<LongWritable,PairText> read) throws Exception {
						parser.parseSequence(read.f1.getLeft().getBytes(), read.f1.getLeft().getLength(), leftBuffer);
						parser.parseSequence(read.f1.getRight().getBytes(), read.f1.getRight().getLength(), rightBuffer);
						tuple3.setFields(read.f0, leftBuffer, rightBuffer);
						return tuple3;
					}
				});

		return inputReadsDS;
	}
}
