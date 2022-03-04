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

import java.util.ArrayList;
import java.util.List;

import es.udc.gac.bigdec.ec.Correction;

public class CandidatePath {
	private List<Correction> modifiedBases;
	private int sumQs;

	public CandidatePath() {
		modifiedBases = new ArrayList<Correction>();
		sumQs = 0;
	}

	public CandidatePath(int sumQs, List<Short> indexes, byte[] bases) {
		this.sumQs = sumQs;
		this.modifiedBases = new ArrayList<Correction>(indexes.size());

		for (int i = 0; i<indexes.size(); i++)
			this.modifiedBases.add(new Correction(indexes.get(i), bases[i]));
	}

	public CandidatePath(CandidatePath path) {
		modifiedBases = new ArrayList<Correction>(path.modifiedBases);
		sumQs = path.sumQs;
	}

	public int getSumQs() {
		return sumQs;
	}

	public void incQuality(int sumQs) {
		this.sumQs += sumQs;
	}

	public void addCorrection(Correction corr) {
		this.modifiedBases.add(corr);
	}

	public List<Correction> getModifiedBases() {
		return modifiedBases;
	}

	public void setSumQs(int sumQs) {
		this.sumQs = sumQs;
	}
}
