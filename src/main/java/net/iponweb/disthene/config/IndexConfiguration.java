package net.iponweb.disthene.config;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Andrei Ivanov
 */
public class IndexConfiguration {

    private String name;
    private String index;
    private String type;
    private List<String> cluster = new ArrayList<>();
    private int port;
    private IndexBulkConfiguration bulk;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public List<String> getCluster() {
        return cluster;
    }

    public void setCluster(List<String> cluster) {
        this.cluster = cluster;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public IndexBulkConfiguration getBulk() {
        return bulk;
    }

    public void setBulk(IndexBulkConfiguration bulk) {
        this.bulk = bulk;
    }

    @Override
    public String toString() {
        return "IndexConfiguration{" +
                "name='" + name + '\'' +
                ", index='" + index + '\'' +
                ", type='" + type + '\'' +
                ", cluster=" + cluster +
                ", port=" + port +
                ", bulk=" + bulk +
                '}';
    }
}
