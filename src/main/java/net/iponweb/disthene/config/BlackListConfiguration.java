package net.iponweb.disthene.config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Andrei Ivanov
 */
public class BlackListConfiguration {

    private Map<String, List<String>> blackListRules;
    private Map<String, List<String>> whiteListRules;

    public BlackListConfiguration(Map<String, List<String>> blackListRules, Map<String, List<String>> whiteListRules) {
        this.blackListRules = blackListRules;
        this.whiteListRules = whiteListRules;
    }

    public Map<String, List<String>> getBlackListRules() {
        return blackListRules;
    }

    public void setBlackListRules(Map<String, List<String>> blackListRules) {
        this.blackListRules = blackListRules;
    }

    public Map<String, List<String>> getWhiteListRules() {
        return whiteListRules;
    }

    public void setWhiteListRules(Map<String, List<String>> whiteListRules) {
        this.whiteListRules = whiteListRules;
    }

    @Override
    public String toString() {
        return "BlackListConfiguration{" +
                "blackListRules=" + blackListRules +
                ", whiteListRules=" + whiteListRules +
                '}';
    }
}
