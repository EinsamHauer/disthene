package net.iponweb.disthene.bus;

import net.iponweb.disthene.events.DistheneEvent;

/**
 * @author Andrei Ivanov
 */
public interface DistheneEventListener {

    void handle(DistheneEvent event);


}
