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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.udc.gac.bigdec.ec.CorrectionAlgorithm;
import es.udc.gac.bigdec.kmer.Kmer;
import es.udc.gac.bigdec.kmer.KmerGenerator;
import es.udc.gac.bigdec.util.Timer;

public final class KmerMap {
	private static final Logger logger = LoggerFactory.getLogger(KmerMap.class);
	private static final float LOAD_FACTOR = .75F;
	private static final String LOAD_KMERS_TIME = "LOAD_KMERS_TIME";
	private static final Timer timer = new Timer();
	private static transient volatile Map<String, KmerMap> kmerMap = new HashMap<String, KmerMap>();

	private transient Map<Kmer, Short> solidKmers;

	private KmerMap() {}

	private KmerMap(int numberOfKmers) { 
		solidKmers = createKmerMap(numberOfKmers);
	}

	private static Map<Kmer, Short> createKmerMap(int numberOfKmers) {
		// Try to avoid costly re-hashing operations
		return new HashMap<Kmer, Short>((int) (Math.ceil((numberOfKmers + 1) / LOAD_FACTOR)), LOAD_FACTOR);
	}

	public synchronized static void loadFromCSV(CorrectionAlgorithm algorithm, String kmersFile) throws IOException  {
		KmerMap map = kmerMap.get(algorithm.toString());

		if (map == null) {
			timer.start(LOAD_KMERS_TIME);
			logger.info("{}: loading k-mers on {} from {}", algorithm, InetAddress.getLocalHost().getHostName(), kmersFile);	
			map = new KmerMap(algorithm.getNumberOfSolidKmers());
			KmerMap.loadKmersFromCSV(kmersFile, algorithm.getKmerThreshold(), map.solidKmers);
			kmerMap.put(algorithm.toString(), map);
			timer.stop(LOAD_KMERS_TIME);
			logger.info("{}: loaded {} k-mers in {} seconds", algorithm, map.solidKmers.size(), timer.getTotalTime(LOAD_KMERS_TIME));
		}

		algorithm.setSolidKmers(map.solidKmers);
	}

	public synchronized static void loadFromBroadcast(CorrectionAlgorithm algorithm, RuntimeContext context,
			short maxCounter) throws IOException  {

		KmerMap map = kmerMap.get(algorithm.toString());

		if (map == null) {
			timer.start(LOAD_KMERS_TIME);
			logger.info("{}: loading k-mers on {}", algorithm, InetAddress.getLocalHost().getHostName());	
			map = new KmerMap(algorithm.getNumberOfSolidKmers());

			List<Tuple2<Kmer,Integer>> kmers = context.getBroadcastVariable("solidKmersDS");

			for (Tuple2<Kmer,Integer> kmer : kmers) {
				if (kmer.f1 >= algorithm.getKmerThreshold()) {
					if (kmer.f1 <= maxCounter) {
						map.solidKmers.put(kmer.f0, kmer.f1.shortValue());
						continue;
					}
					map.solidKmers.put(kmer.f0, maxCounter);
				}
			}

			kmers = null;
			kmerMap.put(algorithm.toString(), map);
			timer.stop(LOAD_KMERS_TIME);
			logger.info("{}: loaded {} k-mers in {} seconds", algorithm, map.solidKmers.size(), timer.getTotalTime(LOAD_KMERS_TIME));
		}

		algorithm.setSolidKmers(map.solidKmers);
	}

	private static void loadKmersFromCSV(String file, short kmerThreshold, Map<Kmer, Short> solidKmers) throws IOException {
		BufferedReader br = null;
		FSDataInputStream dis = null;
		Path path = new Path(file);
		FileSystem fs = path.getFileSystem(Job.getInstance().getConfiguration());
		String line;
		String[] splits;
		short counter;

		dis = fs.open(path);
		br = new BufferedReader(new InputStreamReader(dis));
		line = br.readLine();

		while (line != null) {
			splits = line.split(",");
			counter =  Short.parseShort(splits[1]);
			if (counter >= kmerThreshold) {
				solidKmers.put(KmerGenerator.createKmer(Long.parseLong(splits[0], 10)), counter);
			}
			line = br.readLine();
		}

		if (br != null)
			br.close();
		if (dis != null)
			dis.close();
	}
}
