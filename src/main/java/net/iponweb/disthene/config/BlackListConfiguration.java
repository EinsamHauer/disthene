package net.iponweb.disthene.config;

import java.util.List;
import java.util.Map;

/**
 * @author Andrei Ivanov
 */
public class BlackListConfiguration {

    private final Map<String, List<String>> blackListRules;
    private final Map<String, List<String>> whiteListRules;

    public BlackListConfiguration(Map<String, List<String>> blackListRules, Map<String, List<String>> whiteListRules) {
        this.blackListRules = blackListRules;
        this.whiteListRules = whiteListRules;
    }

    public Map<String, List<String>> getBlackListRules() {
        return blackListRules;
    }

    public Map<String, List<String>> getWhiteListRules() {
        return whiteListRules;
    }

    @Override
    public String toString() {
        return "BlackListConfiguration{" +
                "blackListRules=" + blackListRules +
                ", whiteListRules=" + whiteListRules +
                '}';
    }
}
