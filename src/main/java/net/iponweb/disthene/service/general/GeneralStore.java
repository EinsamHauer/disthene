package net.iponweb.disthene.service.general;

import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;
import net.iponweb.disthene.service.blacklist.BlackList;
import net.iponweb.disthene.service.events.DistheneEvent;
import net.iponweb.disthene.service.events.MetricReceivedEvent;
import net.iponweb.disthene.service.events.MetricStoreEvent;

/**
 * @author Andrei Ivanov
 */
@Listener(references= References.Strong)
public class GeneralStore {

    private MBassador<DistheneEvent> bus;
    private BlackList blackList;

    public GeneralStore(MBassador<DistheneEvent> bus, BlackList blackList) {
        this.bus = bus;
        this.blackList = blackList;
        bus.subscribe(this);
    }

    @Handler(rejectSubtypes = false)
    public void handle(MetricReceivedEvent metricReceivedEvent) {
        if (!blackList.isBlackListed(metricReceivedEvent.getMetric())) {
            bus.post(new MetricStoreEvent(metricReceivedEvent.getMetric())).now();
        }

    }

}
