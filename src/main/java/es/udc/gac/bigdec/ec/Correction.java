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
package es.udc.gac.bigdec.ec;

public class Correction {
	private short index;
	private byte base;

	public Correction() {
		this.index = 0;
		this.base = 0;
	}

	public Correction(short index, byte base) {
		this.index = index;
		this.base = base;
	}

	public short getIndex() {
		return index;
	}

	public byte getBase() {
		return base;
	}

	public void setIndex(short index) {
		this.index = index;
	}

	public void setBase(byte base) {
		this.base = base;
	}
}
