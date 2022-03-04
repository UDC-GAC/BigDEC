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
package es.udc.gac.bigdec.ec.bless2;

public class Trimming {
	private int trim3End;
	private int trim5End;

	public Trimming() {
		trim3End = 0;
		trim5End = 0;
	}

	public Trimming(int trim5End, int trim3End) {
		this.trim3End = trim3End;
		this.trim5End = trim5End;
	}

	public Trimming(Trimming other) {
		this.trim3End = other.trim3End;
		this.trim5End = other.trim5End;
	}

	public int get3End() {
		return trim3End;
	}

	public void set3End(int trim3End) {
		this.trim3End = trim3End;
	}

	public int get5End() {
		return trim5End;
	}

	public void set5End(int trim5End) {
		this.trim5End = trim5End;
	}

	public void clear() {
		trim3End = 0;
		trim5End = 0;
	}
}
