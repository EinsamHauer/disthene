package net.iponweb.disthene;

import net.engio.mbassy.bus.MBassador;
import net.iponweb.disthene.carbon.CarbonServer;
import net.iponweb.disthene.config.AggregationConfiguration;
import net.iponweb.disthene.config.BlackListConfiguration;
import net.iponweb.disthene.config.DistheneConfiguration;
import net.iponweb.disthene.service.aggregate.RollupAggregator;
import net.iponweb.disthene.service.aggregate.SumAggregator;
import net.iponweb.disthene.service.blacklist.BlackList;
import net.iponweb.disthene.service.events.DistheneEvent;
import net.iponweb.disthene.service.general.GeneralStore;
import net.iponweb.disthene.service.index.ESIndexStore;
import net.iponweb.disthene.service.stats.Stats;
import net.iponweb.disthene.service.store.CassandraMetricStore;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

/**
 * @author Andrei Ivanov
 */
public class Disthene {
    private static Logger logger;

    private static final String DEFAULT_CONFIG_LOCATION = "/etc/disthene/disthene.yaml";
    private static final String DEFAULT_BLACKLIST_LOCATION = "/etc/disthene/blacklist.yaml";
    private static final String DEFAULT_AGGREGATION_CONFIG_LOCATION = "/etc/disthene/aggregator.yaml";
    private static final String DEFAULT_LOG_CONFIG_LOCATION = "/etc/disthene/disthene-log4j.xml";

    public static void main(String[] args) throws MalformedURLException {
        Options options = new Options();
        options.addOption("c", "config", true, "config location");
        options.addOption("l", "log-config", true, "log config location");
        options.addOption("b", "blacklist", true, "blacklist location");
        options.addOption("a", "agg-config", true, "aggregation config location");

        CommandLineParser parser = new GnuParser();
        try {
            CommandLine commandLine = parser.parse(options, args);
            System.getProperties().setProperty("log4j.configuration", "file:" + commandLine.getOptionValue("l", DEFAULT_LOG_CONFIG_LOCATION));
            logger = Logger.getLogger(Disthene.class);

            Yaml yaml = new Yaml();
            InputStream in = Files.newInputStream(Paths.get(commandLine.getOptionValue("c", DEFAULT_CONFIG_LOCATION)));
            DistheneConfiguration distheneConfiguration = yaml.loadAs(in, DistheneConfiguration.class);
            in.close();
            logger.info("Running with the following config: " + distheneConfiguration.toString());

            logger.info("Creating event bus");
            MBassador<DistheneEvent> bus = new MBassador<>();

            logger.info("Loading blacklists");
            in = Files.newInputStream(Paths.get(commandLine.getOptionValue("b", DEFAULT_BLACKLIST_LOCATION)));
            BlackListConfiguration blackListConfiguration = new BlackListConfiguration((Map<String, List<String>>) yaml.load(in));
            in.close();
            logger.debug("Running with the following blacklist: " + blackListConfiguration.toString());
            BlackList blackList = new BlackList(blackListConfiguration);

            logger.info("Creating general store");
            new GeneralStore(bus, blackList);

            logger.info("Creating stats");
            new Stats(bus, distheneConfiguration.getStats(), distheneConfiguration.getCarbon().getBaseRollup());

            logger.info("Creating ES index store");
            new ESIndexStore(distheneConfiguration, bus);

            logger.info("Creating Cassandra metric store");
            new CassandraMetricStore(distheneConfiguration.getStore(), bus);

            logger.info("Loading aggregation rules");
            in = Files.newInputStream(Paths.get(commandLine.getOptionValue("a", DEFAULT_AGGREGATION_CONFIG_LOCATION)));
            AggregationConfiguration aggregationConfiguration = new AggregationConfiguration((Map<String, Map<String, String>>) yaml.load(in));
            in.close();
            logger.debug("Running with the following aggregation rule set: " + aggregationConfiguration.toString());
            logger.info("Creating sum aggregator");
            new SumAggregator(bus, distheneConfiguration, aggregationConfiguration, blackList);

            logger.info("Creating rollup aggregator");
            new RollupAggregator(bus, distheneConfiguration, distheneConfiguration.getCarbon().getRollups());

            logger.info("Starting carbon");
            CarbonServer carbonServer = new CarbonServer(distheneConfiguration, bus);
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
