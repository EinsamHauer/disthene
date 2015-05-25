package net.iponweb.disthene.service.blacklist;

import com.google.common.base.Joiner;
import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.config.BlackListConfiguration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Andrei Ivanov
 */
public class BlackList {

    private Map<String, Pattern> rules = new HashMap<>();

    public BlackList(BlackListConfiguration blackListConfiguration) {
        for(Map.Entry<String, List<String>> entry : blackListConfiguration.getRules().entrySet()) {
            rules.put(entry.getKey(), Pattern.compile(Joiner.on("|").skipNulls().join(entry.getValue())));
        }
    }

    public boolean isBlackListed(Metric metric) {
        if (rules.get(metric.getTenant()) != null) {
            Matcher matcher = rules.get(metric.getTenant()).matcher(metric.getPath());
            return matcher.matches();
        } else {
            return false;
        }
    }
}
