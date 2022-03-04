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

public class FastQParser implements SequenceParser {

	private static final long serialVersionUID = -1847778393529477878L;
	public static final String FASTQ_COMMENT_LINE = "\n+\n";

	@Override
	public Sequence parseSequence(byte[] bytes, int length) {
		int offset = 6; // Assuming @ERRXXXXXX, @SRRXXXXXX, @DRRXXXXXX
		Sequence seq = new Sequence();

		// Read the sequence name
		int lineLength = SequenceParser.nextToken(bytes, offset) + 1; //Include line feed
		seq.setName(bytes, 0, lineLength + offset);
		offset += lineLength;

		// Read the sequence bases
		if (!(offset < length)) {
			String read = new String(bytes, offset, length, StandardCharsets.US_ASCII);
			throw new IllegalArgumentException("Wrong Sequence format for " + read
					+ ": only name available");
		}

		lineLength = SequenceParser.nextToken(bytes, offset);
		seq.setBases(bytes, offset, lineLength);
		offset += lineLength + FastQParser.FASTQ_COMMENT_LINE.length(); // Skip line feed and comment line

		// Read the qualities
		if (!(offset < length)) {
			String read = new String(bytes, offset, length, StandardCharsets.US_ASCII);
			throw new IllegalArgumentException("Wrong Sequence format for " + read
					+ ": no qualities available");
		}

		lineLength = SequenceParser.nextToken(bytes, offset);
		seq.setQuals(bytes, offset, lineLength);

		return seq;
	}
}
