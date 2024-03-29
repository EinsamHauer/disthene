package net.iponweb.disthene;

import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.bus.config.BusConfiguration;
import net.engio.mbassy.bus.config.Feature;
import net.iponweb.disthene.carbon.CarbonServer;
import net.iponweb.disthene.config.AggregationConfiguration;
import net.iponweb.disthene.config.BlackListConfiguration;
import net.iponweb.disthene.config.DistheneConfiguration;
import net.iponweb.disthene.events.DistheneEvent;
import net.iponweb.disthene.service.aggregate.RollupService;
import net.iponweb.disthene.service.aggregate.SumService;
import net.iponweb.disthene.service.auth.TenantService;
import net.iponweb.disthene.service.blacklist.BlacklistService;
import net.iponweb.disthene.service.index.IndexService;
import net.iponweb.disthene.service.metric.MetricService;
import net.iponweb.disthene.service.stats.StatsService;
import net.iponweb.disthene.service.store.CassandraService;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.Yaml;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * @author Andrei Ivanov
 */
public class Disthene {
    private static Logger logger;

    private static final String DEFAULT_CONFIG_LOCATION = "/etc/disthene/disthene.yaml";
    private static final String DEFAULT_BLACKLIST_LOCATION = "/etc/disthene/blacklist.yaml";
    private static final String DEFAULT_WHITELIST_LOCATION = "/etc/disthene/whitelist.yaml";
    private static final String DEFAULT_AGGREGATION_CONFIG_LOCATION = "/etc/disthene/aggregator.yaml";
    private static final String DEFAULT_LOG_CONFIG_LOCATION = "/etc/disthene/disthene-log4j.xml";

    private final String configLocation;
    private final String blacklistLocation;
    private final String whitelistLocation;
    private final String aggregationConfigLocation;

    private MBassador<DistheneEvent> bus;
    private BlacklistService blacklistService;
    private StatsService statsService;
    private IndexService indexService;
    private CassandraService cassandraService;
    private SumService sumService;
    private RollupService rollupService;
    private TenantService tenantService;
    private CarbonServer carbonServer;

    public Disthene(String configLocation, String blacklistLocation, String whitelistLocation, String aggregationConfigLocation) {
        this.configLocation = configLocation;
        this.blacklistLocation = blacklistLocation;
        this.whitelistLocation = whitelistLocation;
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
            bus = new MBassador<>(new BusConfiguration()
                    .addFeature(Feature.SyncPubSub.Default())
                    .addFeature(Feature.AsynchronousHandlerInvocation.Default())
                    .addFeature(Feature.AsynchronousMessageDispatch.Default())
                    .addPublicationErrorHandler(error -> logger.error(error))
            );

            logger.info("Loading blacklists & whitelists");
            Map<String, List<String>> blacklistRules = Collections.emptyMap();
            Map<String, List<String>> whitelistRules = Collections.emptyMap();

            if (new File(blacklistLocation).exists()) {
                in = Files.newInputStream(Paths.get(blacklistLocation));
                Map<String, List<String>> map = yaml.load(in);
                if (map != null) blacklistRules = map;
                in.close();
            }

            if (new File(whitelistLocation).exists()) {
                in = Files.newInputStream(Paths.get(whitelistLocation));
                Map<String, List<String>> map = yaml.load(in);
                if (map != null) whitelistRules = map;
                in.close();
            }

            BlackListConfiguration blackListConfiguration = new BlackListConfiguration(blacklistRules, whitelistRules);
            logger.debug("Running with the following blacklist: " + blackListConfiguration);
            blacklistService = new BlacklistService(blackListConfiguration);

            logger.info("Creating metric service");
            @SuppressWarnings("unused")
            MetricService metricService = new MetricService(bus, blacklistService);

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
            AggregationConfiguration aggregationConfiguration = new AggregationConfiguration(yaml.load(in));
            in.close();
            logger.debug("Running with the following aggregation rule set: " + aggregationConfiguration);
            logger.info("Creating sum aggregator");
            sumService = new SumService(bus, distheneConfiguration, aggregationConfiguration, blacklistService);

            logger.info("Creating rollup aggregator");
            rollupService = new RollupService(bus, distheneConfiguration, distheneConfiguration.getCarbon().getRollups());

            logger.info("Creating tenant authentication service");
            tenantService = new TenantService(new HashSet<>(distheneConfiguration.getCarbon().getAuthorizedTenants()), distheneConfiguration.getCarbon().isAllowAll());

            logger.info("Starting carbon");
            carbonServer = new CarbonServer(distheneConfiguration, bus, tenantService);
            carbonServer.run();

            // adding signal handlers
            try {
                Signal.handle(new Signal("HUP"), new SighupSignalHandler());
            } catch (IllegalArgumentException e) {
                logger.warn("HUP signal is not available. Will not handle it");
            }

            try {
                Signal.handle(new Signal("TERM"), new SigtermSignalHandler());
            } catch (IllegalArgumentException e) {
                logger.warn("TERM signal is not available. Will not handle it");
            }
        } catch (IOException e) {
            logger.error(e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("c", "config", true, "config location");
        options.addOption("l", "log-config", true, "log config location");
        options.addOption("b", "blacklist", true, "blacklist location");
        options.addOption("w", "whitelist", true, "whitelist location");
        options.addOption("a", "agg-config", true, "aggregation config location");

        CommandLineParser parser = new DefaultParser();

        try {
            CommandLine commandLine = parser.parse(options, args);
            System.getProperties().setProperty("log4j.configurationFile", "file:" + commandLine.getOptionValue("l", DEFAULT_LOG_CONFIG_LOCATION));
            logger = LogManager.getLogger(Disthene.class);

            new Disthene(commandLine.getOptionValue("c", DEFAULT_CONFIG_LOCATION),
                    commandLine.getOptionValue("b", DEFAULT_BLACKLIST_LOCATION),
                    commandLine.getOptionValue("w", DEFAULT_WHITELIST_LOCATION),
                    commandLine.getOptionValue("a", DEFAULT_AGGREGATION_CONFIG_LOCATION)
            ).run();

        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Disthene", options);
        } catch (Exception e) {
            if (logger != null) {
                logger.fatal("Start failed", e);
            } else {
                System.out.println("Start failed");
                e.printStackTrace();
            }
        }

    }

    private class SighupSignalHandler implements SignalHandler {

        @Override
        public void handle(Signal signal) {
            logger.info("Received sighup");

            logger.info("Reloading blacklists");
            try {
                Yaml yaml = new Yaml();

                Map<String, List<String>> blacklistRules = Collections.emptyMap();
                Map<String, List<String>> whitelistRules = Collections.emptyMap();

                if (new File(blacklistLocation).exists()) {
                    InputStream in = Files.newInputStream(Paths.get(blacklistLocation));
                    Map<String, List<String>> map = yaml.load(in);
                    if (map != null) blacklistRules = map;
                    in.close();
                }

                if (new File(whitelistLocation).exists()) {
                    InputStream in = Files.newInputStream(Paths.get(whitelistLocation));
                    Map<String, List<String>> map = yaml.load(in);
                    if (map != null) whitelistRules = map;
                    in.close();
                }
                InputStream in = Files.newInputStream(Paths.get(blacklistLocation));
                BlackListConfiguration blackListConfiguration = new BlackListConfiguration(blacklistRules, whitelistRules);
                in.close();

                blacklistService.setRules(blackListConfiguration);

                logger.debug("Reloaded blacklist: " + blackListConfiguration);
            } catch (Exception e) {
                logger.error("Reloading blacklists failed");
                logger.error(e);
            }

            logger.info("Reloading aggregation rules");
            try {
                Yaml yaml = new Yaml();
                InputStream in = Files.newInputStream(Paths.get(aggregationConfigLocation));
                AggregationConfiguration aggregationConfiguration = new AggregationConfiguration(yaml.load(in));
                in.close();

                sumService.setAggregationConfiguration(aggregationConfiguration);
                logger.debug("Reloaded aggregation rules: " + aggregationConfiguration);
            } catch (Exception e) {
                logger.error("Reloading aggregation rules failed");
                logger.error(e);
            }

            logger.info("Reloading allowed tenants");
            try {
                Yaml yaml = new Yaml();
                InputStream in = Files.newInputStream(Paths.get(configLocation));
                DistheneConfiguration distheneConfiguration = yaml.loadAs(in, DistheneConfiguration.class);
                in.close();

                tenantService.setRules(new HashSet<>(distheneConfiguration.getCarbon().getAuthorizedTenants()), distheneConfiguration.getCarbon().isAllowAll());
                logger.debug("Reloaded allowed tenants: {allowAll="
                        + distheneConfiguration.getCarbon().isAllowAll()
                        + ", tenants=" + distheneConfiguration.getCarbon().getAuthorizedTenants());
            } catch (Exception e) {
                logger.error("Reloading allowed tenants failed");
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

            // We will probably lose some last stats here. But leaving it to run will complicate things
            logger.info("Shutting down stats service");
            statsService.shutdown();

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
