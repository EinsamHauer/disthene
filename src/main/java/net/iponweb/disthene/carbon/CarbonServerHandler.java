package net.iponweb.disthene.carbon;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.CharsetUtil;
import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.config.Rollup;
import net.iponweb.disthene.service.aggregate.Aggregator;
import net.iponweb.disthene.service.blacklist.BlackList;
import net.iponweb.disthene.service.general.GeneralStore;
import net.iponweb.disthene.service.index.IndexStore;
import net.iponweb.disthene.service.store.MetricStore;
import org.apache.log4j.Logger;

/**
 * @author Andrei Ivanov
 */
public class CarbonServerHandler extends ChannelInboundHandlerAdapter {
    private Logger logger = Logger.getLogger(CarbonServerHandler.class);

    private GeneralStore generalStore;

    public CarbonServerHandler(GeneralStore generalStore, Rollup baseRollup) {
        this.generalStore = generalStore;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        String received = ((ByteBuf) msg).toString(CharsetUtil.UTF_8).trim();
        ((ByteBuf) msg).release();

        // parse it right away
        try {
            String[] splitInput = received.split("\\s");
            generalStore.store(splitInput[3], splitInput[0], Long.parseLong(splitInput[2]), Double.parseDouble(splitInput[1]));

        } catch (Exception e) {
            //discard immediately
            logger.error(e);
            e.printStackTrace();
        }
    }
}
