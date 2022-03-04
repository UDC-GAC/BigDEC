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

public interface SequenceParser extends Serializable {

	public static final byte LF = '\n';
	public static final byte LF_BYTE = '\n';

	public abstract Sequence parseSequence(byte[] bytes, int length);

	public static int nextToken(byte[] bytes, int offset) {
		int posTmp = offset;

		while(bytes[posTmp] != LF)
			posTmp++;

		return posTmp - offset;
	}

	public static int trim(byte[] bytes, int start) {
		int pos = -1;

		for (int i = start; i < bytes.length; i++) {
			if (bytes[i] == ' ') {
				pos = i;
				break;
			}
		}

		if (pos != -1)
			bytes[pos++] = LF_BYTE;

		return pos;
	}
}
