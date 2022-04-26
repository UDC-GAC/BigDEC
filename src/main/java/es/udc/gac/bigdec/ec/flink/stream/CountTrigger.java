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

import java.io.Serializable;

import org.apache.flink.util.Preconditions;

public class CountTrigger<T> implements Serializable {
	private static final long serialVersionUID = -4343892739180507503L;

	private final long maxCount;
	private transient BundleTriggerCallback callback;
	private transient long count = 0;

	public CountTrigger(long maxCount) {
		Preconditions.checkArgument(maxCount > 0, "maxCount must be greater than 0");
		this.maxCount = maxCount;
	}

	public long getMaxCount() {
		return maxCount;
	}

	public void registerCallback(BundleTriggerCallback callback) {
		this.callback = Preconditions.checkNotNull(callback, "callback is null");
	}

	public void onElement(T element) throws Exception {
		count++;
		if (count >= maxCount) {
			callback.finishBundle();
			reset();
		}
	}

	public void reset() {
		count = 0;
	}
}
