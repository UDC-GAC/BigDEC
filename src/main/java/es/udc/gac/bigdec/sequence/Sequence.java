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
package es.udc.gac.bigdec.sequence;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Sequence {

	private byte[] name;
	private byte[] bases;
	private byte[] quals;

	public Sequence() {}

	public void setName(byte[] name) {
		this.name = name;
	}

	public void setBases(byte[] bases) {
		this.bases = bases;
	}

	public void setQuals(byte[] quals) {
		this.quals = quals;
	}

	public void setName(byte[] name, int offset, int length) {
		this.name = new byte[length];
		System.arraycopy(name, offset, this.name, 0, length);
	}

	public void setBases(byte[] bases, int offset, int length) {
		this.bases = new byte[length];
		System.arraycopy(bases, offset, this.bases, 0, length);
	}

	public void setQuals(byte[] quals, int offset, int length) {
		this.quals = new byte[length];
		System.arraycopy(quals, offset, this.quals, 0, length);
	}

	public byte[] getName() {
		return name;
	}

	public byte[] getBases() {
		return bases;
	}

	public byte[] getQuals() {
		return quals;
	}

	public int getLength() {
		return bases.length;
	}

	public String basesToString() {
		return new String(bases, StandardCharsets.US_ASCII);
	}

	@Override
	public int hashCode() {
		return this.bases.hashCode();
	}

	@Override
	public boolean equals(Object other) {
		if (other == null) return false;
		if (!(other instanceof Sequence)) return false;
		if (other == this) return true;
		return Arrays.equals(((Sequence) other).bases, this.bases);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder((bases.length*2) + name.length + 4);
		// Print name, bases and quality scores
		return sb.append(new String(name, 0, name.length, StandardCharsets.US_ASCII))
				.append(new String(bases, StandardCharsets.US_ASCII))
				.append(FastQParser.FASTQ_COMMENT_LINE)
				.append(new String(quals, StandardCharsets.US_ASCII)).toString();
	}
}
