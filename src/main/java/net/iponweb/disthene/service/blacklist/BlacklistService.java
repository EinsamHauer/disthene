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
public class BlacklistService {
    private Map<String, Pattern> blackListRules = new HashMap<>();
    private Map<String, Pattern> whiteListRules = new HashMap<>();

    public BlacklistService(BlackListConfiguration blackListConfiguration) {
        for(Map.Entry<String, List<String>> entry : blackListConfiguration.getBlackListRules().entrySet()) {
            blackListRules.put(entry.getKey(), Pattern.compile(Joiner.on("|").skipNulls().join(entry.getValue())));
        }
        for(Map.Entry<String, List<String>> entry : blackListConfiguration.getWhiteListRules().entrySet()) {
            whiteListRules.put(entry.getKey(), Pattern.compile(Joiner.on("|").skipNulls().join(entry.getValue())));
        }
    }

    public boolean isBlackListed(Metric metric) {
        Pattern pattern = blackListRules.get(metric.getTenant());
        if (pattern != null) {
            Matcher matcher = pattern.matcher(metric.getPath());
            return matcher.matches() && !isWhiteListed(metric);
        } else {
            return false;
        }
    }

    public boolean isWhiteListed(Metric metric) {
        Pattern pattern = whiteListRules.get(metric.getTenant());
        if (pattern != null) {
            Matcher matcher = pattern.matcher(metric.getPath());
            return matcher.matches();
        } else {
            return false;
        }
    }

    public void setRules(BlackListConfiguration blackListConfiguration) {
        Map<String, Pattern> blackListRules = new HashMap<>();

        for(Map.Entry<String, List<String>> entry : blackListConfiguration.getBlackListRules().entrySet()) {
            blackListRules.put(entry.getKey(), Pattern.compile(Joiner.on("|").skipNulls().join(entry.getValue())));
        }

        this.blackListRules = blackListRules;

        Map<String, Pattern> whiteListRules = new HashMap<>();

        for(Map.Entry<String, List<String>> entry : blackListConfiguration.getWhiteListRules().entrySet()) {
            whiteListRules.put(entry.getKey(), Pattern.compile(Joiner.on("|").skipNulls().join(entry.getValue())));
        }

        this.whiteListRules = whiteListRules;

    }
}
