/*******************************************************************************
 * Copyright (c) 2016 
 *
 *   Any part of code or file can be changed,    
 *   redistributed, modified with the copyright  
 *   information intact .                       
 *  
 * Contributors:
 *         Hareesh Makam
 *******************************************************************************/

package org.arrah.dq.spark.core.datasampler.cli;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class Validator {
	private static final Logger logger = Logger.getLogger(Validator.class.getName());
	private String[] args = null;
	public Options options = new Options();

	public Validator(String[] args) {
		this.args = args;

		options.addOption("h", "help", false, "show this help.");
		options.addOption("i", "input", true, "Input Folder/File path");
		options.addOption("o", "output", true, "Output Folder path");
		options.addOption("if", "inputFormat", true, "input file format  - parquet/csv/tab or a delimiter");
		options.addOption("of", "outFormat", true, "output file format - parquet/csv/tab or a delimiter");
		options.addOption("t", "type", true, "Sampling type  - ran	dom/stratified/keylist");
		options.addOption("c", "keyColumn", true, "Key Column for stratified/keylist sampling");
		options.addOption("f", "fraction", true, "Sample fraction size");
		options.addOption("fm", "fractionMapping", true, "comma seperated pairs of key,fraction size");
		options.addOption("hr", "header", true, "true if header is present.");
	}

	public CommandLine parse() {
		CommandLineParser parser = new BasicParser();

		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);

			if (cmd.hasOption("h"))
				help();

			if (cmd.hasOption("i")) {
				// logger.log(Level.INFO, cmd.getOptionValue("i"));
			} else {
				logger.log(Level.SEVERE, "-i Input location is Missing");

				help();
				System.exit(1);
			}

			if (cmd.hasOption("o")) {

			} else {
				logger.log(Level.SEVERE, "-o output localtion is Missing");
				help();
				System.exit(1);
			}

			if (cmd.hasOption("t")) {
				if (cmd.getOptionValue("t").equalsIgnoreCase("stratifed") ||cmd.getOptionValue("t").equalsIgnoreCase("keyList") ) {

					if (cmd.hasOption("c")) {

					} else {
						logger.log(Level.SEVERE, "-c key Column is needed for stratified/keyList sampling");
						help();
						System.exit(1);
					}
					
					if(cmd.hasOption("fm")){
						
					}else{

						logger.log(Level.SEVERE, "-fm key,fraction file is needed for stratified/keylist sampling");
						help();
						System.exit(1);
					}

				}
			} else {
				logger.log(Level.SEVERE, "-t Type of sampling is missing");
				help();
				System.exit(1);
			}

			return cmd;
		} catch (ParseException e) {
			logger.log(Level.SEVERE, "Failed to parse comand line properties", e);
			help();
			System.exit(1);
		}
		return cmd;
	}

	private void help() {
		// This prints out some help
		HelpFormatter formater = new HelpFormatter();

		formater.printHelp("ComputeSample", options);
		System.exit(0);
	}

}
