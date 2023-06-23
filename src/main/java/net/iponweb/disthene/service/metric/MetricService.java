package net.iponweb.disthene.service.metric;

import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;
import net.iponweb.disthene.service.blacklist.BlacklistService;
import net.iponweb.disthene.events.DistheneEvent;
import net.iponweb.disthene.events.MetricReceivedEvent;
import net.iponweb.disthene.events.MetricStoreEvent;

/**
 * @author Andrei Ivanov
 */
@Listener(references= References.Strong)
public class MetricService {

    private final MBassador<DistheneEvent> bus;
    private final BlacklistService blacklistService;

    public MetricService(MBassador<DistheneEvent> bus, BlacklistService blacklistService) {
        this.bus = bus;
        this.blacklistService = blacklistService;
        bus.subscribe(this);
    }

    @SuppressWarnings("unused")
    @Handler()
    public void handle(MetricReceivedEvent metricReceivedEvent) {
        if (!blacklistService.isBlackListed(metricReceivedEvent.getMetric())) {
            bus.post(new MetricStoreEvent(metricReceivedEvent.getMetric())).now();
        }

    }

}
