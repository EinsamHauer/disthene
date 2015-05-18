package net.iponweb.disthene.config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Andrei Ivanov
 */
public class BlackListConfiguration {

    private Map<String, List<String>> rules = new HashMap<>();

    public BlackListConfiguration(Map<String, List<String>> rules) {
        this.rules = rules;
    }

    public Map<String, List<String>> getRules() {
        return rules;
    }

    public void setRules(Map<String, List<String>> rules) {
        this.rules = rules;
    }

    @Override
    public String toString() {
        return "BlackListConfiguration{" +
                "rules=" + rules +
                '}';
    }
}
