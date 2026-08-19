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

public final class HashMix {
	private static final int C1 = 0x85EBCA6B;
	private static final int C2 = 0xC2B2AE35;
	private static final long C3 = 0xff51afd7ed558ccdL;
	private static final long C4 = 0xc4ceb9fe1a85ec53L;
	private static final long C5 = 0x9FB21C651E98DF25L;

	private HashMix() {
	}

	public static int xxHashLong(long k) {
		long h = xxh3Rrmxmx(k);
		return (int) (h ^ (h >>> 32));
	}

	public static int murmurLong(long k) {
		long h = fmix64(k);
		return (int) (h ^ (h >>> 32));
	}

	public static int murmurInt(int k) {
		return fmix32(k);
	}

	// MurmurHash3 32 bits
	private static int fmix32(int h) {
		h ^= h >>> 16;
		h *= C1;
		h ^= h >>> 13;
		h *= C2;
		return h ^ h >>> 16;
	}

	// MurmurHash3 64 bits
	private static long fmix64(long k) {
		k ^= k >>> 33;
		k *= C3;
		k ^= k >>> 33;
		k *= C4;
		return k ^ k >>> 33;
	}

	// XXH3 final avalanche (rrmxmx)
	private static long xxh3Rrmxmx(long k) {
		k ^= Long.rotateRight(k, 49) ^ Long.rotateRight(k, 24);
		k *= C5;
		k ^= k >>> 28;
		k *= C5;
		return k ^ k >>> 28;
	}
}