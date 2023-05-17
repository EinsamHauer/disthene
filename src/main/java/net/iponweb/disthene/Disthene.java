package net.iponweb.disthene;

import com.datastax.driver.core.policies.LatencyAwarePolicy;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.bus.common.Properties;
import net.engio.mbassy.bus.config.BusConfiguration;
import net.engio.mbassy.bus.config.Feature;
import net.engio.mbassy.bus.error.IPublicationErrorHandler;
import net.engio.mbassy.bus.error.PublicationError;
import net.iponweb.disthene.carbon.CarbonServer;
import net.iponweb.disthene.config.AggregationConfiguration;
import net.iponweb.disthene.config.BlackListConfiguration;
import net.iponweb.disthene.config.DistheneConfiguration;
import net.iponweb.disthene.events.DistheneEvent;
import net.iponweb.disthene.service.aggregate.AggregateService;
import net.iponweb.disthene.service.aggregate.RollupService;
import net.iponweb.disthene.service.aggregate.SumService;
import net.iponweb.disthene.service.blacklist.BlacklistService;
import net.iponweb.disthene.service.index.IndexService;
import net.iponweb.disthene.service.metric.MetricService;
import net.iponweb.disthene.service.stats.StatsService;
import net.iponweb.disthene.service.store.CassandraService;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;
import org.yaml.snakeyaml.Yaml;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
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

    public static Feature.AsynchronousMessageDispatch dispatch;

    private static final String DEFAULT_CONFIG_LOCATION = "/etc/disthene/disthene.yaml";
    private static final String DEFAULT_BLACKLIST_LOCATION = "/etc/disthene/blacklist.yaml";
    private static final String DEFAULT_AGGREGATION_CONFIG_LOCATION = "/etc/disthene/aggregator.yaml";
    private static final String DEFAULT_LOG_CONFIG_LOCATION = "/etc/disthene/disthene-log4j.xml";

    private String configLocation;
    private String blacklistLocation;
    private String aggregationConfigLocation;

    private MBassador<DistheneEvent> bus;
    private BlacklistService blacklistService;
    private MetricService metricService;
    private AggregateService aggregateService;
    private StatsService statsService;
    private IndexService indexService;
    private CassandraService cassandraService;
    private SumService sumService;
    private RollupService rollupService;
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

            dispatch = Feature.AsynchronousMessageDispatch.Default();
            logger.info("Creating dispatcher");
            bus = new MBassador<>(new BusConfiguration()
                    .addFeature(Feature.SyncPubSub.Default())
                    .addFeature(Feature.AsynchronousHandlerInvocation.Default())
                    .addFeature(dispatch)
                    .setProperty(Properties.Handler.PublicationError, new IPublicationErrorHandler() {
                        @Override
                        public void handleError(PublicationError error) {
                            logger.error(error);
                        }
                    })
            );

            logger.info("Loading blacklists");
            in = Files.newInputStream(Paths.get(blacklistLocation));
            BlackListConfiguration blackListConfiguration = new BlackListConfiguration((Map<String, List<String>>) yaml.load(in));
            in.close();
            logger.debug("Running with the following blacklist: " + blackListConfiguration.toString());
            blacklistService = new BlacklistService(blackListConfiguration);

            logger.info("Creating metric service");
            metricService = new MetricService(bus, blacklistService, distheneConfiguration);

            logger.info("Creating base rollup aggregator");
            aggregateService = new AggregateService(bus, distheneConfiguration);

            logger.info("Creating stats");
            statsService = new StatsService(bus, distheneConfiguration.getStats(), distheneConfiguration.getCarbon().getBaseRollup());

            try {
                MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
                ObjectName name = new ObjectName("net.iponweb.disthene.service:type=Stats");
                mbs.registerMBean(statsService, name);
            } catch (Exception e) {
                logger.error("Failed to create MBean: " + e);
            }

            logger.info("Creating ES index service");
            indexService = new IndexService(distheneConfiguration.getIndex(), bus);

            logger.info("Creating C* service");
            cassandraService = new CassandraService(distheneConfiguration.getStore(), bus);

            logger.info("Loading aggregation rules");
            in = Files.newInputStream(Paths.get(aggregationConfigLocation));
            AggregationConfiguration aggregationConfiguration = new AggregationConfiguration((Map<String, Map<String, String>>) yaml.load(in));
            in.close();
            logger.debug("Running with the following aggregation rule set: " + aggregationConfiguration.toString());
            logger.info("Creating sum aggregator");
            sumService = new SumService(bus, distheneConfiguration, aggregationConfiguration, blacklistService);

            logger.info("Creating rollup aggregator");
            rollupService = new RollupService(bus, distheneConfiguration, distheneConfiguration.getCarbon().getRollups());

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

                blacklistService.setRules(blackListConfiguration);

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

                sumService.setAggregationConfiguration(aggregationConfiguration);
                logger.debug("Reloaded aggregation rules: " + aggregationConfiguration.toString());
            } catch (Exception e) {
                logger.error("Reloading aggregation rules failed");
                logger.error(e);
            }

            logger.info("Invalidating index cache");
            indexService.invalidateCache();
        }
    }

    private class SigtermSignalHandler implements SignalHandler {

        @Override
        public void handle(Signal signal) {
            logger.info("Shutting down carbon server");
            carbonServer.shutdown();

            // We will probably loose some last stats here. But leaving it to run will complicate things
            logger.info("Shutting down stats service");
            statsService.shutdown();

	    logger.info("Shutting down base rollup aggregator");
            aggregateService.shutdown();

            logger.info("Shutting down sum aggregator");
            sumService.shutdown();

            logger.info("Shutting down rollup aggregator");
            rollupService.shutdown();

            // Now let's wait for this all to propagate and shutdown the bus
            logger.info("Shutting down dispatcher");
            bus.shutdown();

            // Now flush what's left and shutdown
            logger.info("Shutting down ES service");
            indexService.shutdown();

            logger.info("Shutting down C* service");
            cassandraService.shutdown();

            logger.info("Shutdown complete");

            System.exit(0);
        }
    }
}
