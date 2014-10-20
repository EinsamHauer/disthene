package net.iponweb.disthene;

import net.iponweb.disthene.config.CarbonConfiguration;
import net.iponweb.disthene.config.DistheneConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.config.ConfigFileApplicationListener;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.FileSystemResource;
import org.yaml.snakeyaml.Yaml;

import javax.annotation.Resource;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * @author Andrei Ivanov
 */

@Configuration
@EnableAutoConfiguration()
@ComponentScan
@EnableConfigurationProperties(CarbonConfiguration.class)
public class Disthene implements CommandLineRunner {

    @Value("${config:/etc/disthene/disthene.yml}")
    private String config;

    @Autowired
    private DistheneConfiguration configuration;

    @Override
    public void run(String... args) throws Exception {
        configuration.parseConfiguration(config);
        System.out.println("Hello World!" + config + " / ");// + configuration.getPort());
    }

/*
    @Bean
    public PropertySource<?> yamlPropertySourceLoader() throws IOException {
        YamlPropertySourceLoader loader = new YamlPropertySourceLoader();
        PropertySource<?> applicationYamlPropertySource = loader.load("application.yml", new FileSystemResource(config),null);
        return applicationYamlPropertySource;
    }
*/

    public static void main(String[] args) {
        SpringApplication.run(Disthene.class, args);
    }
}
