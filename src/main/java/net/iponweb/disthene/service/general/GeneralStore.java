package net.iponweb.disthene.service.general;

import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;
import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.service.blacklist.BlackList;
import net.iponweb.disthene.service.events.MetricIndexEvent;
import net.iponweb.disthene.service.events.MetricReceivedEvent;
import net.iponweb.disthene.service.events.MetricStoreEvent;
import org.apache.log4j.Logger;

/**
 * @author Andrei Ivanov
 */
@Listener(references= References.Strong)
public class GeneralStore {

    private Logger logger = Logger.getLogger(GeneralStore.class);

    private MBassador bus;
    private BlackList blackList;

    public GeneralStore(MBassador bus, BlackList blackList) {
        this.bus = bus;
        this.blackList = blackList;
        bus.subscribe(this);
    }

    @Handler(rejectSubtypes = false)
    public void handle(MetricReceivedEvent metricReceivedEvent) {
        if (!blackList.isBlackListed(metricReceivedEvent.getMetric())) {
            bus.post(new MetricStoreEvent(metricReceivedEvent.getMetric())).now();
            bus.post(new MetricIndexEvent(metricReceivedEvent.getMetric())).now();
        }

    }

}
