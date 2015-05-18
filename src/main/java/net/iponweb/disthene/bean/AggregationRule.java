package net.iponweb.disthene.bean;

import java.util.regex.Pattern;

/**
 * @author Andrei Ivanov
 */
public class AggregationRule {

    private Pattern source;
    private String destination;

    public AggregationRule(String sourceDefinition, String destination) {
        this.destination = destination;
        source = Pattern.compile(sourceDefinition.replace(".", "\\.").replace("*", ".*?").replaceAll("<([a-z]+)>", "(?<$1>.*?)"));
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

    @Override
    public String toString() {
        return "AggregationRule{" +
                "source=" + source +
                ", destination='" + destination + '\'' +
                '}';
    }
}
