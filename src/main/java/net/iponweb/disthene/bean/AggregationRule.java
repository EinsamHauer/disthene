package net.iponweb.disthene.bean;

import java.util.regex.Pattern;

/**
 * @author Andrei Ivanov
 */
public class AggregationRule {

    private final Pattern source;
    private final String prefix;

    public AggregationRule(String sourceDefinition, String destination) {
        source = Pattern.compile(sourceDefinition.replace(".", "\\.").replace("*", ".*?").replaceAll("<([a-z]+)>", "(?<$1>.*?)"));
        prefix = destination.split("<data>",-1)[0];
    }

    public Pattern getSource() {
        return source;
    }

    public String getPrefix() {
        return prefix;
    }

    @Override
    public String toString() {
        return "AggregationRule{" +
                "source=" + source +
                ", prefix='" + prefix + '\'' +
                '}';
    }
}
