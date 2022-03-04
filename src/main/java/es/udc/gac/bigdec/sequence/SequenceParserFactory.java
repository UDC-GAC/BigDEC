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

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public final class SequenceParserFactory {

	public enum FileFormat {
		FILE_FORMAT_FASTA, FILE_FORMAT_FASTQ, FILE_FORMAT_UNKNOWN
	}

	private SequenceParserFactory() {}

	public static SequenceParser createParser(FileFormat format) {
		if (format == FileFormat.FILE_FORMAT_FASTQ)
			return new FastQParser();
		else if (format == FileFormat.FILE_FORMAT_FASTA)
			throw new IllegalArgumentException("FASTA format is not currently supported");
		else
			throw new IllegalArgumentException("Unrecognized file format ("+format+")");
	}

	public static FileFormat autoDetectFileFormat(FileSystem fs, Path file) throws IOException {
		FileFormat format = FileFormat.FILE_FORMAT_UNKNOWN;

		FSDataInputStream reader = fs.open(file);

		// Read first byte
		char firstChar = (char) reader.read();

		if (firstChar == -1) {
			throw new IOException("Unrecognized file format");
		} else if (firstChar == '@') {
			format = FileFormat.FILE_FORMAT_FASTQ;
		} else if (firstChar == '>') {
			format = FileFormat.FILE_FORMAT_FASTA;        	
		} else {
			throw new IOException("Unrecognized file format");
		}

		reader.close();
		return format;
	}
}
