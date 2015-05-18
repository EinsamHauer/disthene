package net.iponweb.disthene;

import net.iponweb.disthene.carbon.CarbonServer;
import net.iponweb.disthene.config.DistheneConfiguration;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * @author Andrei Ivanov
 */

public class Disthene {
    private static Logger logger;

    private static final String DEFAULT_CONFIG_LOCATION = "/etc/disthene/disthene.yml";
    private static final String DEFAULT_LOG_CONFIG_LOCATION = "/etc/disthene/disthene-log4j.xml";

    public static void main(String[] args) throws MalformedURLException {
        Options options = new Options();
        options.addOption("c", "config", true, "config location");
        options.addOption("l", "log-config", true, "log config location");

        CommandLineParser parser = new GnuParser();
        try {
            CommandLine commandLine = parser.parse(options, args);
            System.out.println(commandLine.getOptionValue("c", DEFAULT_CONFIG_LOCATION));
            System.getProperties().setProperty("log4j.configuration", "file:" + commandLine.getOptionValue("l", DEFAULT_LOG_CONFIG_LOCATION));
            logger = Logger.getLogger(Disthene.class);

            Yaml yaml = new Yaml();
            InputStream in = Files.newInputStream(Paths.get(commandLine.getOptionValue("c", DEFAULT_CONFIG_LOCATION)));
            DistheneConfiguration distheneConfiguration = yaml.loadAs(in, DistheneConfiguration.class);
            logger.info("Running with the following config: " + distheneConfiguration.toString());

            logger.info("Starting carbon");
            CarbonServer carbonServer = new CarbonServer(distheneConfiguration);
            carbonServer.run();


        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Disthene", options);
        } catch (IOException e) {
            logger.error(e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
