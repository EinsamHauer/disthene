package net.iponweb.disthene.service.metric;

import net.iponweb.disthene.bus.DistheneBus;
import net.iponweb.disthene.bus.DistheneEventListener;
import net.iponweb.disthene.events.DistheneEvent;
import net.iponweb.disthene.events.MetricReceivedEvent;
import net.iponweb.disthene.events.MetricStoreEvent;
import net.iponweb.disthene.service.blacklist.BlacklistService;

/**
 * @author Andrei Ivanov
 */
public class MetricService implements DistheneEventListener {

    private DistheneBus bus;
    private BlacklistService blacklistService;

    public MetricService(DistheneBus bus, BlacklistService blacklistService) {
        this.bus = bus;
        this.blacklistService = blacklistService;
        bus.subscribe(MetricReceivedEvent.class, this);
    }

    @Override
    public void handle(DistheneEvent event) {
        if (event instanceof MetricReceivedEvent) {
            if (!blacklistService.isBlackListed(((MetricReceivedEvent) event).getMetric())) {
                bus.post(new MetricStoreEvent(((MetricReceivedEvent) event).getMetric()));
            }
        }
    }
}
