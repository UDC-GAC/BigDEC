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

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.io.LongWritable;

import es.udc.gac.bigdec.ec.CorrectionAlgorithm;
import es.udc.gac.bigdec.ec.flink.KmerMap;
import es.udc.gac.bigdec.sequence.Sequence;

public class CorrectSingle extends RichMapFunction<Tuple2<LongWritable,Sequence>,Tuple2<LongWritable,Sequence>> {
	private static final long serialVersionUID = -1991173889717482138L;

	private CorrectionAlgorithm algorithm;
	private String kmersFile;
	private boolean fromFile;
	private short maxCounter;

	public CorrectSingle(CorrectionAlgorithm algorithm, boolean fromFile, String kmersFile, short maxCounter) {
		this.algorithm = algorithm;
		this.kmersFile = kmersFile;
		this.fromFile = fromFile;
		this.maxCounter = maxCounter;
	}

	@Override
	public void open(org.apache.flink.configuration.Configuration config) throws Exception {
		if (fromFile)
			KmerMap.loadFromCSV(algorithm, kmersFile);
		else
			KmerMap.loadFromBroadcast(algorithm, getRuntimeContext(), maxCounter);
	}

	@Override
	public Tuple2<LongWritable,Sequence> map(Tuple2<LongWritable,Sequence> sequence) throws Exception {
		algorithm.correctRead(sequence.f1);
		return sequence;
	}
}
