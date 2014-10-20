package net.iponweb.disthene.config;

import org.springframework.boot.bind.YamlJavaBeanPropertyConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.Loader;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * @author Andrei Ivanov
 */
@Component
public final class DistheneConfiguration {
    private CarbonConfiguration carbon;

    public CarbonConfiguration getCarbon() {
        return carbon;
    }

    public void setCarbon(CarbonConfiguration carbon) {
        this.carbon = carbon;
    }

    public void parseConfiguration(String path) throws IOException {
        Constructor constructor = new YamlJavaBeanPropertyConstructor(DistheneConfiguration.class);
        Yaml yaml = new Yaml(new Loader(constructor));
        InputStream in = Files.newInputStream(Paths.get(path));
        Object o = yaml.load(in);
        System.out.println(o.toString() );
    }
}
