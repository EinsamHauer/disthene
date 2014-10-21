package net.iponweb.disthene;

import net.iponweb.disthene.config.CarbonConfiguration;
import net.iponweb.disthene.config.DistheneConfiguration;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * @author Andrei Ivanov
 */

@Configuration
@EnableAutoConfiguration()
@ComponentScan
@EnableConfigurationProperties(CarbonConfiguration.class)
public class Disthene implements CommandLineRunner {
    Logger logger = Logger.getLogger(Disthene.class);

    private static final String DEFAULT_CONFIG_LOCATION = "/etc/disthene/disthene.yml";
    private static final String DEFAULT_LOG_CONFIG_LOCATION = "/etc/disthene/disthene-log4j.xml";

    @Autowired
    private DistheneConfiguration configuration;

    @Override
    public void run(String... args) throws Exception {
        logger.error("aaaaaaaaaaaaaaaaaaaaaaaaaaa");
        System.out.println("Hello World!" + " / " + configuration.getCarbon().getBind().getHostAddress());// + configuration.getPort());
    }

    @SuppressWarnings("static-access")
    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("c", "config", true, "config location");
        options.addOption("l", "log-config", true, "log config location");

        CommandLineParser parser = new GnuParser();
        try {
            CommandLine commandLine = parser.parse(options, args);
            System.getProperties().setProperty("spring.config.location", "file:" + commandLine.getOptionValue("c", DEFAULT_CONFIG_LOCATION));
            System.getProperties().setProperty("logging.config", "file:" + commandLine.getOptionValue("l", DEFAULT_LOG_CONFIG_LOCATION));

            SpringApplication springApplication = new SpringApplication(Disthene.class);
            springApplication.setShowBanner(false);
            springApplication.run(args);
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Disthene", options);
        }
    }
}
