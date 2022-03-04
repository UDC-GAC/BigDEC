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

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.apache.spark.api.java.function.MapPartitionsFunction;

import es.udc.gac.bigdec.sequence.Sequence;
import scala.Tuple2;

public class QsHistogramPaired implements MapPartitionsFunction<Tuple2<Sequence,Sequence>,int[]>{

	private static final long serialVersionUID = -3504704906883678129L;

	private int histogramSize;

	public QsHistogramPaired(int histogramSize) {
		this.histogramSize = histogramSize;
	}

	@Override
	public Iterator<int[]> call(Iterator<Tuple2<Sequence,Sequence>> pairedSequences) throws Exception {
		if (!pairedSequences.hasNext())
			return Collections.emptyIterator();

		int[] histogram = new int[histogramSize];
		byte[] qualsLeft,qualsRight;
		Tuple2<Sequence,Sequence> pairedSeq;
		int i;

		while(pairedSequences.hasNext()) {
			pairedSeq = pairedSequences.next();
			qualsLeft = pairedSeq._1.getQuals();
			qualsRight = pairedSeq._2.getQuals();

			for (i=0; i<pairedSeq._1.getLength();i++) {
				histogram[qualsLeft[i]]++;
				histogram[qualsRight[i]]++;
			}
		}

		return Arrays.asList(histogram).iterator();
	}
}
