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
import sun.misc.Signal;
import sun.misc.SignalHandler;

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

    private String configLocation;
    private String blacklistLocation;
    private String aggregationConfigLocation;

    private MBassador<DistheneEvent> bus;
    private BlackList blackList;
    private GeneralStore generalStore;
    private Stats stats;
    private ESIndexStore esIndexStore;
    private CassandraMetricStore cassandraMetricStore;
    private SumAggregator sumAggregator;
    private RollupAggregator rollupAggregator;
    private CarbonServer carbonServer;

    public Disthene(String configLocation, String blacklistLocation, String aggregationConfigLocation) {
        this.configLocation = configLocation;
        this.blacklistLocation = blacklistLocation;
        this.aggregationConfigLocation = aggregationConfigLocation;
    }

    private void run() {
        try {
            Yaml yaml = new Yaml();
            InputStream in = Files.newInputStream(Paths.get(configLocation));
            DistheneConfiguration distheneConfiguration = yaml.loadAs(in, DistheneConfiguration.class);
            in.close();
            logger.info("Running with the following config: " + distheneConfiguration.toString());

            logger.info("Creating dispatcher");
            bus = new MBassador<>();

            logger.info("Loading blacklists");
            in = Files.newInputStream(Paths.get(blacklistLocation));
            BlackListConfiguration blackListConfiguration = new BlackListConfiguration((Map<String, List<String>>) yaml.load(in));
            in.close();
            logger.debug("Running with the following blacklist: " + blackListConfiguration.toString());
            blackList = new BlackList(blackListConfiguration);

            logger.info("Creating general store");
            generalStore = new GeneralStore(bus, blackList);

            logger.info("Creating stats");
            stats = new Stats(bus, distheneConfiguration.getStats(), distheneConfiguration.getCarbon().getBaseRollup());

            logger.info("Creating ES index store");
            esIndexStore = new ESIndexStore(distheneConfiguration, bus);

            logger.info("Creating C* metric store");
            cassandraMetricStore = new CassandraMetricStore(distheneConfiguration.getStore(), bus);

            logger.info("Loading aggregation rules");
            in = Files.newInputStream(Paths.get(aggregationConfigLocation));
            AggregationConfiguration aggregationConfiguration = new AggregationConfiguration((Map<String, Map<String, String>>) yaml.load(in));
            in.close();
            logger.debug("Running with the following aggregation rule set: " + aggregationConfiguration.toString());
            logger.info("Creating sum aggregator");
            sumAggregator = new SumAggregator(bus, distheneConfiguration, aggregationConfiguration, blackList);

            logger.info("Creating rollup aggregator");
            rollupAggregator = new RollupAggregator(bus, distheneConfiguration, distheneConfiguration.getCarbon().getRollups());

            logger.info("Starting carbon");
            carbonServer = new CarbonServer(distheneConfiguration, bus);
            carbonServer.run();

            // adding signal handlers
            Signal.handle(new Signal("HUP"), new SighupSignalHandler());

            Signal.handle(new Signal("TERM"), new SigtermSignalHandler());
        } catch (IOException e) {
            logger.error(e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

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

            new Disthene(commandLine.getOptionValue("c", DEFAULT_CONFIG_LOCATION),
                    commandLine.getOptionValue("b", DEFAULT_BLACKLIST_LOCATION),
                    commandLine.getOptionValue("a", DEFAULT_AGGREGATION_CONFIG_LOCATION)
            ).run();

        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Disthene", options);
        } catch (Exception e) {
            System.out.println("Start failed");
            e.printStackTrace();
        }

    }

    private class SighupSignalHandler implements SignalHandler {

        @Override
        public void handle(Signal signal) {
            logger.info("Received sighup");

            logger.info("Reloading blacklists");
            try {
                Yaml yaml = new Yaml();
                InputStream in = Files.newInputStream(Paths.get(blacklistLocation));
                BlackListConfiguration blackListConfiguration = new BlackListConfiguration((Map<String, List<String>>) yaml.load(in));
                in.close();

                blackList.setRules(blackListConfiguration);

                logger.debug("Reloaded blacklist: " + blackListConfiguration.toString());
            } catch (Exception e) {
                logger.error("Reloading blacklists failed");
                logger.error(e);
            }

            logger.info("Reloading aggregation rules");
            try {
                Yaml yaml = new Yaml();
                InputStream in = Files.newInputStream(Paths.get(aggregationConfigLocation));
                AggregationConfiguration aggregationConfiguration = new AggregationConfiguration((Map<String, Map<String, String>>) yaml.load(in));
                in.close();

                sumAggregator.setAggregationConfiguration(aggregationConfiguration);
                logger.debug("Reloaded aggregation rules: " + aggregationConfiguration.toString());
            } catch (Exception e) {
                logger.error("Reloading aggregation rules failed");
                logger.error(e);
            }
        }
    }

    private class SigtermSignalHandler implements SignalHandler {

        @Override
        public void handle(Signal signal) {
            logger.info("Shutting down carbon server");
            carbonServer.shutdown();

            logger.info("Shutting down dispatcher");
            bus.shutdown();

            logger.info("Shutting down sum aggregator");
            sumAggregator.shutdown();

            logger.info("Shutting down rollup aggregator");
            rollupAggregator.shutdown();

            logger.info("Shutting down rollup ES index service");
            esIndexStore.shutdown();

            logger.info("Shutting down rollup C* index service");
            cassandraMetricStore.shutdown();

            logger.info("Shutdown complete");
            System.exit(0);

        }
    }
}
