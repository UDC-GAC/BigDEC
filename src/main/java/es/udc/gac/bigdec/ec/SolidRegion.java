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

public class SolidRegion {
	private short leftKmer;
	private short rightKmer;

	public SolidRegion() {}

	public SolidRegion(SolidRegion solidRegion) {
		this.leftKmer = solidRegion.leftKmer;
		this.rightKmer = solidRegion.rightKmer;
	}

	public SolidRegion(int leftKmer, int rightKmer) {
		this.leftKmer = (short)leftKmer;
		this.rightKmer = (short)rightKmer;
	}

	public short length() {
		return (short) (rightKmer - leftKmer + 1);
	}

	public short first() {
		return leftKmer;
	}

	public short getLeftKmer() {
		return leftKmer;
	}

	public short second() {
		return rightKmer;
	}

	public short getRightKmer() {
		return rightKmer;
	}

	public void first(int leftKmer) {
		this.leftKmer = (short) leftKmer;
	}

	public void setLeftKmer(int leftKmer) {
		this.leftKmer = (short) leftKmer;
	}

	public void second(int rightKmer) {
		this.rightKmer = (short) rightKmer;
	}

	public void setRightKmer(int rightKmer) {
		this.rightKmer = (short) rightKmer;
	}
}
