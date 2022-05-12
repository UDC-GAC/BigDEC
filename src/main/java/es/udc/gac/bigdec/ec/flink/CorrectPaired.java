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

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.hadoop.io.LongWritable;

import es.udc.gac.bigdec.ec.CorrectionAlgorithm;
import es.udc.gac.bigdec.ec.flink.KmerMap;
import es.udc.gac.bigdec.sequence.Sequence;

public class CorrectPaired extends RichMapFunction<Tuple3<LongWritable,Sequence,Sequence>,Tuple3<LongWritable,Sequence,Sequence>> {
	private static final long serialVersionUID = -4662420520884558538L;

	private CorrectionAlgorithm algorithm;
	private String kmersFile;
	private boolean fromFile;
	private short maxCounter;

	public CorrectPaired(CorrectionAlgorithm algorithm, boolean fromFile, String kmersFile, short maxCounter) {
		this.algorithm = algorithm;
		this.kmersFile = kmersFile;
		this.fromFile = fromFile;
		this.maxCounter = maxCounter;
	}

	@Override
	public void open(org.apache.flink.configuration.Configuration config) throws Exception {
		if (fromFile)
			KmerMap.loadFromCSV(algorithm, kmersFile, maxCounter);
		else
			KmerMap.loadFromBroadcast(algorithm, getRuntimeContext(), maxCounter);
	}

	@Override
	public Tuple3<LongWritable,Sequence,Sequence> map(Tuple3<LongWritable,Sequence,Sequence> sequence) throws Exception {
		algorithm.correctRead(sequence.f1);
		algorithm.correctRead(sequence.f2);
		return sequence;
	}
}
