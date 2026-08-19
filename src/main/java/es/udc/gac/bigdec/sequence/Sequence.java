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

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Sequence implements Serializable {

	private byte[] name;
	private byte[] bases;
	private byte[] quals;
    private short length;
	private short nameLength;
	
	public Sequence() {
	}

	public void setName(byte[] name) {
		this.name = name;
	}

	public void setBases(byte[] bases) {
		this.bases = bases;
	}

	public void setQuals(byte[] quals) {
		this.quals = quals;
	}

	public void setLength(short length) {
    	this.length = length;
	}

	public void setNameLength(short nameLength) {
    	this.nameLength = nameLength;
	}

    public void setName(byte[] name, int offset, int length) {
        if (this.name == null || this.name.length < length) {
        	this.name = new byte[length];
        }
		this.nameLength = (short) length;
        System.arraycopy(name, offset, this.name, 0, length);
    }

    public void setBases(byte[] bases, int offset, int length) {
        if (this.bases == null || this.bases.length < length) {
        	this.bases = new byte[length];
        }
        this.length = (short) length;
        System.arraycopy(bases, offset, this.bases, 0, length);
    }

    public void setQuals(byte[] quals, int offset, int length) {
    	if (this.quals == null || this.quals.length < length) {
        	this.quals = new byte[length];
        }
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

	public short getLength() {
		return length;
	}

	public short getNameLength() {
    	return nameLength;
	}
	
	public String nameToString() {
		return name == null ? "" : new String(name, 0, nameLength, StandardCharsets.US_ASCII);
	}
	
	public String basesToString() {
		return bases == null ? "" : new String(bases, 0, length, StandardCharsets.US_ASCII);
	}

	public String qualsToString() {
		return quals == null ? "" : new String(quals, 0, length, StandardCharsets.US_ASCII);
    }

	@Override
	public int hashCode() {
	    int h = 1;

    	for (int i = 0; i < length; i++) {
        	h = 31 * h + bases[i];
    	}

    	return h;
	}

	@Override
	public boolean equals(Object obj) {
    	if (this == obj)
        	return true;

    	if (!(obj instanceof Sequence))
        	return false;

    	Sequence other = (Sequence) obj;

    	if (length != other.length)
        	return false;

    	for (int i = 0; i < length; i++) {
        	if (bases[i] != other.bases[i])
            	return false;
    	}

    	return true;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder((length * 2) + nameLength  + 4);
		// Print name, bases and quality scores
		return sb.append(nameToString())
				.append(basesToString())
				.append(FastQParser.FASTQ_COMMENT_LINE)
				.append(qualsToString()).toString();
	}
}
