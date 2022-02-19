package net.iponweb.disthene.bean;

import java.util.regex.Pattern;

/**
 * @author Andrei Ivanov
 */
public class AggregationRule {

    private Pattern source;
    private String destination;
    private final String prefix;

    public AggregationRule(String sourceDefinition, String destination) {
        this.destination = destination;
        source = Pattern.compile(sourceDefinition.replace(".", "\\.").replace("*", ".*?").replaceAll("<([a-z]+)>", "(?<$1>.*?)"));
        prefix = destination.split("<data>",-1)[0];
    }

    public Pattern getSource() {
        return source;
    }

    public void setSource(Pattern source) {
        this.source = source;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public String getPrefix() {
        return prefix;
    }

    @Override
    public String toString() {
        return "AggregationRule{" +
                "source=" + source +
                ", destination='" + destination + '\'' +
                ", prefix='" + prefix + '\'' +
                '}';
    }
}
