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
import java.nio.charset.StandardCharsets;

public final class Kmer implements Comparable<Kmer>, Serializable {

	private static final long serialVersionUID = 9090276500905269140L;

	public static final int NUM_NUCLEOTIDES = 4;
	public static final byte ABASE = 0;			//'A' 65
	public static final byte CBASE = 1;			//'C' 67
	public static final byte GBASE = 2;			//'G' 71
	public static final byte TBASE = 3;			//'T' 84
	private static final byte NBASE = ABASE;	//'N'
	private static final byte ABASEC = TBASE;	// 'A' => 'T'
	private static final byte CBASEC = GBASE;	// 'C' => 'G'
	private static final byte GBASEC = CBASE;	// 'G' => 'C'
	private static final byte TBASEC = ABASE;	// 'T' => 'A'
	private static final byte NBASEC = ABASEC;
	private static final long ZERO = 0L;

	public static final byte[] ENCODE = {
			NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, 
			NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, 
			NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, 
			NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, 
			NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, 
			NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, 
			NBASE, NBASE, NBASE, NBASE, NBASE, ABASE, NBASE, CBASE, NBASE, NBASE, 
			NBASE, GBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, 
			NBASE, NBASE, NBASE, NBASE, TBASE, NBASE, NBASE, NBASE, NBASE, NBASE, 
			NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, ABASE, NBASE, CBASE,
			NBASE, NBASE, NBASE, GBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, 
			NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, TBASE, NBASE, NBASE, NBASE, 
			NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE
	};

	private static final byte[] ENCODEC = {
			NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, 
			NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, 
			NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, 
			NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, 
			NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, 
			NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, 
			NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, ABASEC, NBASEC, CBASEC, NBASEC, NBASEC, 
			NBASEC, GBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, 
			NBASEC, NBASEC, NBASEC, NBASEC, TBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, 
			NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, ABASEC, NBASEC, CBASEC,
			NBASEC, NBASEC, NBASEC, GBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, 
			NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, TBASEC, NBASEC, NBASEC, NBASEC, 
			NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC, NBASEC
	};

	public static final byte[] DECODE = {'A','C','G','T'};

	private static final byte[] SHIFT = {
			62, 60, 58, 56, 54, 52, 50, 48, 46, 44, 42, 40, 38, 36, 34, 32,
			30, 28, 26, 24, 22, 20, 18, 16, 14, 12, 10,  8,  6,  4,  2,  0
	};

	private static final long[] SHIFT_MASK = { 0L,
			0xC000000000000000L, 0xF000000000000000L, 0xFC00000000000000L, 0xFF00000000000000L, 
			0xFFC0000000000000L, 0xFFF0000000000000L, 0xFFFC000000000000L, 0xFFFF000000000000L,
			0xFFFFC00000000000L, 0xFFFFF00000000000L, 0xFFFFFC0000000000L, 0xFFFFFF0000000000L, 
			0xFFFFFFC000000000L, 0xFFFFFFF000000000L, 0xFFFFFFFC00000000L, 0xFFFFFFFF00000000L,
			0xFFFFFFFFC0000000L, 0xFFFFFFFFF0000000L, 0xFFFFFFFFFC000000L, 0xFFFFFFFFFF000000L,
			0xFFFFFFFFFFC00000L, 0xFFFFFFFFFFF00000L, 0xFFFFFFFFFFFC0000L, 0xFFFFFFFFFFFF0000L,
			0xFFFFFFFFFFFFC000L, 0xFFFFFFFFFFFFF000L, 0xFFFFFFFFFFFFFC00L, 0xFFFFFFFFFFFFFF00L,
			0xFFFFFFFFFFFFFFC0L, 0xFFFFFFFFFFFFFFF0L, 0xFFFFFFFFFFFFFFFCL, 0xFFFFFFFFFFFFFFFFL
	};

	private long bases;

	public Kmer() {
		bases = 0L;
	}

	public Kmer(long bases) {
		this.bases = bases;
	}

	public long getBases() {
		return bases;
	}

	public void setBases(long bases) {
		this.bases = bases;
	}

	public void set(Kmer km) {
		this.bases = km.bases;
	}

	public void set(byte[] seqBases, int offset, int kmerLength) {
		clear();
		for(int i = 0; i < kmerLength; i++)
			insertBase(ENCODE[seqBases[i+offset]], i);
	}

	public void setRC(byte[] seqBases, int offset, int kmerLength) {
		clear();
		final int len = kmerLength - 1;
		for(int i = 0; i < kmerLength; i++)
			insertBase(ENCODEC[seqBases[i+offset]], len - i);
	}

	public void clear() {
		bases = ZERO;
	}

	public void setBase(byte base, int pos) {
		bases &= ~(3L << SHIFT[pos]);
		insertBase(base, pos);
	}

	public byte getBase(int pos) {
		return (byte) ((bases >> SHIFT[pos]) & 0x03);
	}

	private void insertBase(byte base, int pos) {
		bases += ((long)base) << SHIFT[pos];
	}

	public void forwardBase(byte base, int kmerLength) {
		// shift left 2 bits
		bases <<= 2;
		insertBase(ENCODE[base], kmerLength - 1);
	}

	public void forwardBaseRC(byte base, int kmerLength) {
		// shift right 2 bits
		bases >>>= 2;
		insertBase(ENCODEC[base], 0);
		bases &= SHIFT_MASK[kmerLength];
	}

	public void backwardBase(byte base, int kmerLength) {
		// shift right 2 bits
		bases >>>= 2;
		insertBase(ENCODE[base], 0);
		bases &= SHIFT_MASK[kmerLength];
	}

	public boolean load(byte[] bases, int offset, int kmerLength) {
		clear();
		byte base;

		for(int i = 0; i < kmerLength; i++) {
			base = bases[i+offset];

			if(Kmer.isNBase(base)) {
				clear();
				return false;
			}

			insertBase(ENCODE[base], i);
		}
		return true;
	}

	public void store(byte[] bases, int kmerLength) {
		for(int i = 0; i < kmerLength; i++)
			bases[i] = DECODE[getBase(i)];
	}

	public void toReverse(Kmer km, int kmerLength) {
		clear();
		final int len = kmerLength >> 1;
		final int kLen = kmerLength - 1;
		for(int i = 0; i < len; i++) {
			insertBase(km.getBase(kLen - i), i);
			insertBase(km.getBase(i), kLen - i);
		}

		if ((kmerLength & 1) != 0)
			insertBase(km.getBase(len), len);
	}

	public void toComplement(Kmer km, int kmerLength) {
		bases = ~km.getBases() & SHIFT_MASK[kmerLength];
	}

	public void toRC(Kmer km, int kmerLength) {
		toReverse(km, kmerLength);
		toComplement(this, kmerLength);
	}

	public void twin(Kmer km, int kmerLength) {
		toRC(km, kmerLength);
	}

	public boolean lower(Kmer other) {
		return this.compareTo(other) < 0;
	}

	@Override
	public int compareTo(Kmer other) {
		return Long.compareUnsigned(bases, other.bases);
	}

	public String basesToString(int kmerLength) {
		byte[] bases = new byte[kmerLength];
		store(bases, kmerLength);
		return new String(bases, StandardCharsets.US_ASCII);
	}

	@Override
	public String toString() {
		return Long.toString(bases);
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof Kmer)
			return bases == ((Kmer) other).bases;
		return false;
	}

	@Override
	public int hashCode() {
		return (int)(bases ^ (bases >>> 32));
	}

	public static boolean isNBase(final byte base) {
		return ((base & 0x5F) == 78);
	}
}