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
package es.udc.gac.bigdec.kmer;

import java.io.Serializable;

import es.udc.gac.bigdec.util.MurmurHash3;

public final class KmerKey implements Comparable<KmerKey>,Serializable {

	private static final long serialVersionUID = 7764883088225952323L;

	private long bases;

	public KmerKey() {
		bases = 0L;
	}

	public KmerKey(long bases) {
		this.bases = bases;
	}

	public long getBases() {
		return bases;
	}

	public void setBases(long bases) {
		this.bases = bases;
	}

	@Override
	public int compareTo(KmerKey other) {
		return Long.compareUnsigned(bases, other.bases);
	}

	@Override
	public String toString() {
		return Long.toString(bases);
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof KmerKey)
			return bases == ((KmerKey)other).bases;
		return false;
	}

	@Override
	public int hashCode() {
		return MurmurHash3.hashLong(bases);
	}
}