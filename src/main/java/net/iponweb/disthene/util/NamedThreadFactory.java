package net.iponweb.disthene.util;

import java.util.concurrent.ThreadFactory;

/**
 * @author Andrei Ivanov
 */
public class NamedThreadFactory implements ThreadFactory {

    private int counter = 0;
    private String baseName;

    public NamedThreadFactory(String baseName) {
        this.baseName = baseName;
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public Thread newThread(Runnable r) {
        return new Thread(r, baseName + "-" + (counter++));
    }
}
