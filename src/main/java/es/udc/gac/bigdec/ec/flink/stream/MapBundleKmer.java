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
package es.udc.gac.bigdec.ec.flink.stream;

import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.udc.gac.bigdec.kmer.Kmer;

public class MapBundleKmer extends MapBundleFunction<Kmer, Integer, Tuple2<Kmer, Integer>, Tuple2<Kmer, Integer>> {
	private static final long serialVersionUID = -4457463922328881937L;
	private static final Logger logger = LoggerFactory.getLogger(MapBundleKmer.class);

	@Override
	public Integer addInput(Integer value, Tuple2<Kmer, Integer> input) throws Exception {
		if (logger.isDebugEnabled())
			logger.debug("processing input {}, value {}", input, value);

		return (value == null)? input.f1 : input.f1 + value;
	}

	@Override
	public void finishBundle(Map<Kmer, Integer> buffer, Collector<Tuple2<Kmer, Integer>> out) throws Exception {
		for (Map.Entry<Kmer, Integer> entry : buffer.entrySet()) {
			out.collect(Tuple2.of(entry.getKey(), entry.getValue()));
		}
	}
}
