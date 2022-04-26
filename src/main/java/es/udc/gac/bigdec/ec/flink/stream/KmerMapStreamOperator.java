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

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.udc.gac.bigdec.kmer.Kmer;
import es.udc.gac.bigdec.kmer.KmerGenerator;

public class KmerMapStreamOperator extends AbstractStreamOperator<Tuple2<Kmer,Integer>> 
implements OneInputStreamOperator<Kmer, Tuple2<Kmer,Integer>>, BundleTriggerCallback {
	private static final Logger logger = LoggerFactory.getLogger(KmerMapStreamOperator.class);
	private static final long serialVersionUID = 1L;
	private static final float LOAD_FACTOR = .75F;

	/** The map in heap to store elements. */
	private final Map<Kmer, MutableInt> kmerMap;

	/** The trigger that determines how many elements should be put into a bundle. */
	private final CountTrigger<Kmer> trigger;

	/** Output for stream records. */
	private transient TimestampedCollector<Tuple2<Kmer,Integer>> collector;

	public KmerMapStreamOperator(CountTrigger<Kmer> trigger) {
		chainingStrategy = ChainingStrategy.ALWAYS;
		this.trigger = checkNotNull(trigger, "trigger is null");
		int size = (int) (Math.ceil((trigger.getMaxCount() + 1) / LOAD_FACTOR));
		this.kmerMap = new HashMap<Kmer, MutableInt>(size, LOAD_FACTOR);
		logger.info("Limit {}, KmerMap size {} ", trigger.getMaxCount(), size);
	}

	@Override
	public void open() throws Exception {
		super.open();
		collector = new TimestampedCollector<>(output);
		trigger.registerCallback(this);
		// reset trigger
		trigger.reset();
	}

	public void finish() throws Exception {
		logger.info("Finishing k-mer map stream operator: {} elements", kmerMap.size());
		finishBundle();
	}

	@Override
	public void processElement(StreamRecord<Kmer> element) throws Exception {
		final Kmer kmer = element.getValue();
		final MutableInt counter = kmerMap.get(kmer);

		if (counter == null)
			kmerMap.put(KmerGenerator.createKmer(kmer), new MutableInt(1));
		else
			counter.increment();

		trigger.onElement(null);
	}

	@Override
	public void finishBundle() throws Exception {
		if (!kmerMap.isEmpty()) {
			if (logger.isDebugEnabled())
				logger.debug("Collecting {} k-mers", kmerMap.size());

			for (Map.Entry<Kmer, MutableInt> entry : kmerMap.entrySet()) {
				collector.collect(Tuple2.of(entry.getKey(), entry.getValue().getValue()));
			}

			kmerMap.clear();
		}
		trigger.reset();
	}
}
