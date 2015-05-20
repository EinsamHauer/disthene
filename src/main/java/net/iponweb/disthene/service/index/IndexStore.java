package net.iponweb.disthene.service.index;

import net.iponweb.disthene.bean.Metric;

import java.io.IOException;

/**
 * @author Andrei Ivanov
 */
public interface IndexStore {

    void store(Metric metric) throws IOException;
}
