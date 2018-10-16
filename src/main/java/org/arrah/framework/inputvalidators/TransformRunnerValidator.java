package org.arrah.framework.inputvalidators;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class TransformRunnerValidator {

	private static final Logger logger = Logger.getLogger(TransformRunnerValidator.class.getName());
	private String[] args = null;
	public Options options = new Options();

	public TransformRunnerValidator(String[] args) {
		this.args = args;

		options.addOption("h", "help", false, "show this help.");
		options.addOption("c", "config", true, "JSON config file for job run.");
		options.addOption("s", "startDate", true, "Start Date for input data");
		options.addOption("e", "endDate", true, "End Date for input data");
		
	}

	public CommandLine parse() {
		CommandLineParser parser = new BasicParser();

		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);

			if (cmd.hasOption("h"))
				this.help("TransformRunner");
 			
		} catch (ParseException e) {
			logger.log(Level.SEVERE, "Failed to parse comand line properties", e);
			help("TransformRunner");
			System.exit(1);
		}
		return cmd;
	}

	private void help(String clazz) {
		
		HelpFormatter formater = new HelpFormatter();

		formater.printHelp(clazz, options);
		System.exit(0);
	}
}
