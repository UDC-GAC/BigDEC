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
package es.udc.gac.bigdec.util;

import java.util.HashMap;
import java.util.Map;

public class Timer {

	private static final double TENTO9 = Double.valueOf(1000000000L);

	private final Map<String, Long> startTimes;
	private final Map<String, Long> elapsedTimes;
	private final Map<String, Long> totalTimes;

	public Timer() {
		startTimes = new HashMap<>();
		elapsedTimes = new HashMap<>();
		totalTimes = new HashMap<>();
	}

	public void start(String name) {
		startTimes.put(name, System.nanoTime());
	}

	public void stop(String name) {
		Long current = System.nanoTime();
		Long starting = startTimes.get(name);

		if (starting == null) {
			elapsedTimes.put(name, 0L);
			totalTimes.put(name, 0L);
			return;
		}

		Long total = totalTimes.get(name);

		if (total == null)
			total = 0L;

		elapsedTimes.put(name, (current - starting));
		totalTimes.put(name, (total + elapsedTimes.get(name)));	
	}

	public double getElapsedTime(String name) {
		Long elapsed = elapsedTimes.get(name);

		if (elapsed == null) {
			return 0;
		}

		return elapsed / Timer.TENTO9;
	}

	public double getTotalTime(String name) {
		Long total = totalTimes.get(name);

		if (total == null) {
			return 0;
		}

		return total / Timer.TENTO9;
	}

	public void reset(String name) {
		startTimes.put(name, 0L);
		elapsedTimes.put(name, 0L);
		totalTimes.put(name, 0L);
	}

	public void resetAll() {
		for (String key : startTimes.keySet()) {
			reset(key);
		}
		for (String key : elapsedTimes.keySet()) {
			reset(key);
		}
		for (String key : totalTimes.keySet()) {
			reset(key);
		}
	}
}