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

	protected static final double MAX_SPLITS = 1.25;

	public FlinkEC(Configuration config, CLIOptions options) {
		super(config, options);
	}

	public static DataSet<Tuple2<LongWritable,Sequence>> parseSingleDS(DataSet<Tuple2<LongWritable,Text>> inputDS,
			SequenceParser parser) {

		DataSet<Tuple2<LongWritable,Sequence>> inputReadsDS = 
				inputDS.map(new MapFunction<Tuple2<LongWritable,Text>, Tuple2<LongWritable,Sequence>>() {

					private static final long serialVersionUID = 1590147064464369799L;
					private Tuple2<LongWritable,Sequence> tuple2 = new Tuple2<LongWritable,Sequence>();

					@Override
					public Tuple2<LongWritable,Sequence> map(Tuple2<LongWritable,Text> read) throws Exception {
						tuple2.setFields(read.f0, parser.parseSequence(read.f1.getBytes(), read.f1.getLength()));
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
					private Tuple2<LongWritable,Sequence> tuple2 = new Tuple2<LongWritable,Sequence>();

					@Override
					public Tuple2<LongWritable,Sequence> map(Tuple2<LongWritable,Text> read) throws Exception {
						tuple2.setFields(read.f0, parser.parseSequence(read.f1.getBytes(), read.f1.getLength()));
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
					private Tuple3<LongWritable,Sequence,Sequence> tuple3 = new Tuple3<LongWritable,Sequence,Sequence>();

					@Override
					public Tuple3<LongWritable,Sequence,Sequence> map(Tuple2<LongWritable,PairText> read) throws Exception {
						Sequence left = parser.parseSequence(read.f1.getLeft().getBytes(), read.f1.getLeft().getLength());
						Sequence right = parser.parseSequence(read.f1.getRight().getBytes(), read.f1.getRight().getLength());
						tuple3.setFields(read.f0, left, right);
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
					private Tuple3<LongWritable,Sequence,Sequence> tuple3 = new Tuple3<LongWritable,Sequence,Sequence>();

					@Override
					public Tuple3<LongWritable,Sequence,Sequence> map(Tuple2<LongWritable,PairText> read) throws Exception {
						Sequence left = parser.parseSequence(read.f1.getLeft().getBytes(), read.f1.getLeft().getLength());
						Sequence right = parser.parseSequence(read.f1.getRight().getBytes(), read.f1.getRight().getLength());
						tuple3.setFields(read.f0, left, right);
						return tuple3;
					}
				});

		return inputReadsDS;
	}
}
