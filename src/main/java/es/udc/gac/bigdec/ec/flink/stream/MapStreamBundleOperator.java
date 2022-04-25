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

import static org.apache.flink.util.Preconditions.checkNotNull;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapStreamBundleOperator <K, V, IN, OUT>
extends AbstractUdfStreamOperator<OUT, MapBundleFunction<K, V, IN, OUT>> 
implements OneInputStreamOperator<IN, OUT>, BundleTriggerCallback {
	private static final Logger logger = LoggerFactory.getLogger(MapStreamBundleOperator.class);
	private static final long serialVersionUID = 1L;
	private static final float LOAD_FACTOR = .75F;

	/** The map in heap to store elements. */
	private final Map<K, V> bundle;

	/** The trigger that determines how many elements should be put into a bundle. */
	private final CountBundleTrigger<IN> trigger;

	/** Output for stream records. */
	private transient TimestampedCollector<OUT> collector;

	/** KeySelector is used to extract key for bundle map. */
	private final KeySelector<IN, K> keySelector;

	private transient int numOfElements = 0;

	public MapStreamBundleOperator(MapBundleFunction<K, V, IN, OUT> function, CountBundleTrigger<IN> trigger, KeySelector<IN, K> keySelector) {
		super(function);
		chainingStrategy = ChainingStrategy.ALWAYS;
		this.trigger = checkNotNull(trigger, "trigger is null");
		this.keySelector = checkNotNull(keySelector, "keySelector is null");
		int size = (int) (Math.ceil((trigger.getMaxCount() + 1) / LOAD_FACTOR));
		this.bundle = new HashMap<K, V>(size, LOAD_FACTOR);
		logger.info("Limit {}, Map size {} ", trigger.getMaxCount(), size);
	}

	@Override
	public void open() throws Exception {
		super.open();

		numOfElements = 0;
		collector = new TimestampedCollector<>(output);

		trigger.registerCallback(this);
		// reset trigger
		trigger.reset();
	}

	public void finish() throws Exception {
		logger.info("Finishing stream operator: {} elements", bundle.size());
		finishBundle();
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		// get the key and value for the map bundle
		final IN input = element.getValue();
		final K bundleKey = getKey(input);		
		final V bundleValue = this.bundle.get(bundleKey);

		// get a new value after adding this element to bundle
		final V newBundleValue = userFunction.addInput(bundleValue, input);

		// update to map bundle
		bundle.put(bundleKey, newBundleValue);

		numOfElements++;
		trigger.onElement(input);
	}

	/**
	 * Get the key for current processing element, which will be used as the map
	 * bundle's key.
	 */
	protected K getKey(IN input) throws Exception {
		return this.keySelector.getKey(input);
	}

	@Override
	public void finishBundle() throws Exception {
		if (!bundle.isEmpty()) {
			numOfElements = 0;
			userFunction.finishBundle(bundle, collector);
			bundle.clear();
		}
		trigger.reset();
	}
}
