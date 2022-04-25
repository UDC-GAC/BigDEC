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

import javax.annotation.Nullable;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.util.Collector;

public abstract class MapBundleFunction<K, V, IN, OUT> implements Function {

	private static final long serialVersionUID = 3034447909940154208L;

	/**
	 * Adds the given input to the given value, returning the new bundle value.
	 *
	 * @param value the existing bundle value, maybe null
	 * @param input the given input, not null
	 * @throws Exception 
	 */
	public abstract V addInput(@Nullable V value, IN input) throws Exception;

	/**
	 * Called when a bundle is finished. Transform a bundle to zero, one, or more
	 * output elements.
	 */
	public abstract void finishBundle(Map<K, V> buffer, Collector<OUT> out) throws Exception;
}
