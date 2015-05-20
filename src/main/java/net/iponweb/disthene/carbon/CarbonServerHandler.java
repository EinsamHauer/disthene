package net.iponweb.disthene.carbon;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.CharsetUtil;
import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.config.Rollup;
import net.iponweb.disthene.service.aggregate.Aggregator;
import net.iponweb.disthene.service.blacklist.BlackList;
import net.iponweb.disthene.service.index.IndexStore;
import net.iponweb.disthene.service.store.MetricStore;
import org.apache.log4j.Logger;

/**
 * @author Andrei Ivanov
 */
public class CarbonServerHandler extends ChannelInboundHandlerAdapter {
    private Logger logger = Logger.getLogger(CarbonServerHandler.class);

    private MetricStore metricStore;
    private IndexStore indexStore;
    private BlackList blackList;
    private Aggregator aggregator;
    private Rollup baseRollup;

    public CarbonServerHandler(MetricStore metricStore, IndexStore indexStore, Rollup baseRollup, BlackList blackList, Aggregator aggregator) {
        this.metricStore = metricStore;
        this.indexStore = indexStore;
        this.baseRollup = baseRollup;
        this.blackList = blackList;
        this.aggregator = aggregator;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf in = (ByteBuf) msg;
        Metric metric = new Metric(in.toString(CharsetUtil.UTF_8).trim(), baseRollup);
        in.release();

        // aggregate
        aggregator.aggregate(metric);

        if (blackList.isBlackListed(metric)) {
            logger.debug("Blacklisted: " + metric.getPath());
        } else {
            indexStore.store(metric);
            metricStore.store(metric);
        }
    }
}
