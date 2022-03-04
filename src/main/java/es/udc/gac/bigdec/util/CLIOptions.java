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
package es.udc.gac.bigdec.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import es.udc.gac.bigdec.RunEC;
import es.udc.gac.bigdec.kmer.KmerGenerator;

public class CLIOptions {

	private static final String header = "\nOptions:\n";
	private static final String footer = "\nMore information available at "+Configuration.WEBPAGE+".\n\n";
	private static final String usage = RunEC.class.getCanonicalName()+
			" -s <file> | -p <file1> <file2> [-o <dir>] [-c <file>] [-k <int>] [-sc <int>] [-m [<dir>]] [-mtoff] [-h] [-v]";

	private CommandLineParser parser = null;
	private Options options = null;
	private HelpFormatter helpFormatter = null;
	private String inputFile1 = null;
	private String inputFile2 = null;
	private String outputDir = null;
	private String mergeOutputDir = null;
	private byte kmerLength = 21;
	private int splitsPerCore = 8;
	private boolean paired = false;
	private boolean merge = false;
	private boolean mergerThread = true;
	private String configPath = null;

	public CLIOptions() {
		options = new Options();
		parser = new DefaultParser();
		helpFormatter = new HelpFormatter();
		helpFormatter.setOptionComparator(null);

		Option single = Option.builder("s")
				.longOpt("single")
				.desc("Input file in FASTQ/FASTA format (single-end mode)")
				.argName("file")
				.hasArg()
				.numberOfArgs(1)
				.build();
		Option paired = Option.builder("p")
				.longOpt("paired")
				.desc("Forward and reverse input files in FASTQ/FASTA format (paired-end mode)")
				.argName("file1> <file2")
				.hasArg()
				.numberOfArgs(2)
				.build();

		OptionGroup optgrp1 = new OptionGroup();
		optgrp1.setRequired(true);
		optgrp1.addOption(single);
		optgrp1.addOption(paired);
		options.addOptionGroup(optgrp1);

		options.addOption(Option.builder("o")
				.longOpt("output-dir")
				.desc("Output directory")
				.argName("dir")
				.hasArg()
				.numberOfArgs(1)
				.required(false)
				.build());

		options.addOption(Option.builder("c")
				.longOpt("config")
				.desc("Configuration file")
				.argName("file")
				.hasArg()
				.numberOfArgs(1)
				.required(false)
				.build());

		options.addOption(Option.builder("k")
				.longOpt("kmer-length")
				.desc("Length of k-mers (default = "+kmerLength+")")
				.argName("int")
				.hasArg()
				.numberOfArgs(1)
				.required(false)
				.build());

		options.addOption(Option.builder("sc")
				.longOpt("splits-core")
				.desc("Input splits per core (default = "+splitsPerCore+" splits)")
				.argName("int")
				.hasArg()
				.numberOfArgs(1)
				.required(false)
				.build());

		options.addOption(Option.builder("m")
				.longOpt("merge")
				.desc("Merge output (output directory is optional. Use file:/ scheme for local file system)")
				.argName("dir")
				.optionalArg(true)
				.numberOfArgs(1)
				.required(false)
				.build());

		options.addOption(Option.builder("mtoff")
				.longOpt("merger-thread-off")
				.desc("Turn off merger thread")
				.hasArg(false)
				.required(false)
				.build());

		options.addOption(Option.builder("h")
				.longOpt("help")
				.desc("Print the help message and exit")
				.hasArg(false)
				.required(false)
				.build());

		options.addOption(Option.builder("v")
				.longOpt("version")
				.desc("Print the version information and exit")
				.hasArg(false)
				.required(false)
				.build());
	}

	public String getInputFile1() {
		return inputFile1;
	}

	public String getInputFile2() {
		return inputFile2;
	}

	public String getOutputDir() {
		return outputDir;
	}

	public void setOutputDir(String outputDir) {
		this.outputDir = outputDir;
	}

	public String getMergeOutputDir() {
		return mergeOutputDir;
	}

	public void setMergeOutputDir(String mergeOutputDir) {
		this.mergeOutputDir = mergeOutputDir;
	}

	public byte getKmerLength() {
		return kmerLength;
	}

	public int getSplitsPerCore() {
		return splitsPerCore;
	}

	public String getConfigPath() {
		return configPath;
	}

	public boolean isPaired() {
		return paired;
	}

	public boolean getMerge() {
		return merge;
	}

	public void setMerge(boolean merge) {
		this.merge = merge;
	}

	public boolean runMergerThread() {
		return mergerThread;
	}

	public void printUsage() {
		helpFormatter.printHelp(87, usage, header, options, footer, false);
	}

	public void parse(String[] args) {
		boolean inputFile = false;

		try {
			CommandLine cmd = parser.parse(this.options, args, false);

			if (cmd.hasOption("h")) {
				printUsage();
				System.exit(-1);
			}

			if (cmd.hasOption("v")) {
				System.out.print("\n"+RunEC.APP_NAME+" "+Configuration.VERSION);
				System.out.print(footer);
				System.exit(-1);
			}

			if (cmd.hasOption("s")) {
				paired = false;
				inputFile = true;
				inputFile1 = cmd.getOptionValue("s");
			}

			if (cmd.hasOption("p")) {
				paired = true;
				inputFile = true;
				String files[] = cmd.getOptionValues("p");
				inputFile1 = files[0];
				inputFile2 = files[1];
			}

			if (cmd.hasOption("o"))
				outputDir = cmd.getOptionValue("o");

			if (cmd.hasOption("c"))
				configPath = cmd.getOptionValue("c");

			if (cmd.hasOption("k"))
				kmerLength = Byte.parseByte(cmd.getOptionValue("k"));

			if (cmd.hasOption("sc"))
				splitsPerCore = Integer.parseInt(cmd.getOptionValue("sc"));

			if (cmd.hasOption("m")) {
				merge = true;

				if (cmd.getOptionValue("m") != null)
					mergeOutputDir = cmd.getOptionValue("m");
			}

			if (cmd.hasOption("mtoff"))
				mergerThread = false;
		} catch (ParseException e) {
			printUsage();
			IOUtils.error(e.getLocalizedMessage());
		}

		if (!inputFile) {
			printUsage();
			IOUtils.error("No input file has been specified");
		}

		if (kmerLength < KmerGenerator.MIN_KMER_SIZE) {
			IOUtils.warn("k-mer length (-k "+kmerLength+") must be >= "
					+KmerGenerator.MIN_KMER_SIZE+". Using -k "+KmerGenerator.MIN_KMER_SIZE+" instead");
			kmerLength = KmerGenerator.MIN_KMER_SIZE;
		}

		if (kmerLength > KmerGenerator.MAX_KMER_SIZE) {
			IOUtils.warn("k-mer length (-k "+kmerLength+") cannot exceed "
					+KmerGenerator.MAX_KMER_SIZE+". Using -k "+KmerGenerator.MAX_KMER_SIZE+" instead");
			kmerLength = KmerGenerator.MAX_KMER_SIZE;
		}

		if (splitsPerCore < 1) {
			IOUtils.warn("the number of input splits per core (-sc "+splitsPerCore+") cannot be less than 1. Using default value instead");
			splitsPerCore = 8;
		}

		if (!merge) {
			mergerThread = false;
			mergeOutputDir = null;
		}
	}
}
