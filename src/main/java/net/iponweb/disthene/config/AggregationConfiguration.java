package net.iponweb.disthene.config;

import net.iponweb.disthene.bean.AggregationRule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Andrei Ivanov
 */
public class AggregationConfiguration {

    private final Map<String, List<AggregationRule>> rules = new HashMap<>();

    public AggregationConfiguration(Map<String, Map<String, String>> rulesDefinition) {
        for(Map.Entry<String, Map<String, String>> entry : rulesDefinition.entrySet()) {
            if (!rules.containsKey(entry.getKey())) {
                rules.put(entry.getKey(), new ArrayList<>());
            }

            for(Map.Entry<String, String> def : entry.getValue().entrySet()) {
                rules.get(entry.getKey()).add(new AggregationRule(def.getKey(), def.getValue()));
            }
        }

    }

    public Map<String, List<AggregationRule>> getRules() {
        return rules;
    }

    @Override
    public String toString() {
        return "AggregationConfiguration{" +
                "rules=" + rules +
                '}';
    }
}
