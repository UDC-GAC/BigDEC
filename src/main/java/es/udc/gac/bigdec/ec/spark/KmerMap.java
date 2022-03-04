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
package es.udc.gac.bigdec.ec.spark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.udc.gac.bigdec.kmer.Kmer;
import es.udc.gac.bigdec.kmer.KmerGenerator;
import es.udc.gac.bigdec.util.Timer;

public final class KmerMap {
	private static final Logger logger = LoggerFactory.getLogger(KmerMap.class);
	private static final float LOAD_FACTOR = .75F;
	private static final String LOAD_KMERS_TIME = "LOAD_KMERS_TIME";
	private static final String FILTER_KMERS_TIME = "FILTER_KMERS_TIME";
	private static final String REMOVE_KMERS_TIME = "REMOVE_KMERS_TIME";
	private static final Timer timer = new Timer();
	private static transient volatile KmerMap kmerMap = null;

	private transient Map<Kmer, Short> solidKmers;
	private transient AtomicBoolean filtered;
	private transient CountDownLatch filteringComplete;

	private KmerMap() {}

	private KmerMap(int numberOfKmers) {
		solidKmers = createKmerMap(numberOfKmers);
		filtered = new AtomicBoolean(false);
		filteringComplete = new CountDownLatch(1);
	}

	private static Map<Kmer, Short> createKmerMap(int numberOfKmers) {
		// Try to avoid costly re-hashing operations
		return new HashMap<Kmer, Short>((int) (Math.ceil((numberOfKmers + 1) / LOAD_FACTOR)), LOAD_FACTOR);
	}

	public synchronized static void load(String kmersFile, int numberOfKmers) throws IOException {
		if (kmerMap == null) {
			timer.start(LOAD_KMERS_TIME);
			logger.info("loading k-mers on {} from {}", InetAddress.getLocalHost().getHostName(), kmersFile);
			kmerMap = new KmerMap(numberOfKmers);
			KmerMap.loadKmersFromCSV(kmersFile, kmerMap.solidKmers);
			timer.stop(LOAD_KMERS_TIME);
			logger.info("loaded {} k-mers in {} seconds", kmerMap.solidKmers.size(), timer.getTotalTime(LOAD_KMERS_TIME));
		}
	}

	public synchronized static void remove() throws IOException {
		if (kmerMap == null)
			return;

		timer.start(REMOVE_KMERS_TIME);
		kmerMap.solidKmers.clear();
		kmerMap.solidKmers = null;
		kmerMap = null;
		timer.stop(REMOVE_KMERS_TIME);
		logger.info("removed k-mers in {} seconds", timer.getTotalTime(REMOVE_KMERS_TIME));
	}

	public static void filter(short kmerThreshold) throws IOException, InterruptedException {
		if (kmerMap == null)
			throw new NullPointerException();

		if (!kmerMap.filtered.getAndSet(true)) {
			timer.start(FILTER_KMERS_TIME);
			logger.info("filtering k-mers on {} (current k-mers: {}, threshold: {})", InetAddress.getLocalHost().getHostName(),
					kmerMap.solidKmers.size(), kmerThreshold);
			// Parallel filtering
			kmerMap.solidKmers = kmerMap.solidKmers.entrySet().parallelStream()
					.filter(x -> x.getValue() >= kmerThreshold)
					.collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue()));
			timer.stop(FILTER_KMERS_TIME);
			logger.info("k-mers filtered in {} seconds (current k-mers: {})", timer.getTotalTime(FILTER_KMERS_TIME), kmerMap.solidKmers.size());
			kmerMap.filteringComplete.countDown();
		} else {
			kmerMap.filteringComplete.await();
		}

		if (kmerMap.filtered.getAndSet(false)) {
			kmerMap.filteringComplete = new CountDownLatch(1);
		}
	}

	public static Map<Kmer, Short> getSolidKmers() {
		if (kmerMap == null)
			throw new NullPointerException();

		return kmerMap.solidKmers;
	}

	public static void loadKmersFromCSV(String file, Map<Kmer, Short> solidKmers) throws IOException {
		BufferedReader br = null;
		FSDataInputStream dis = null;
		Path path = new Path(file);
		FileSystem fs = path.getFileSystem(Job.getInstance().getConfiguration());
		String line;
		String[] splits;

		dis = fs.open(path);
		br = new BufferedReader(new InputStreamReader(dis));
		line = br.readLine();

		while (line != null) {
			splits = line.split(",");
			solidKmers.put(KmerGenerator.createKmer(Long.parseLong(splits[0], 10)), Short.parseShort(splits[1]));
			line = br.readLine();
		}

		if (br != null)
			br.close();
		if (dis != null)
			dis.close();
	}
}
