package learn.cassendra.betterreadsdataloader;

import learn.cassendra.betterreadsdataloader.connection.DataStaxAstraConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.nio.file.Path;

@EnableConfigurationProperties(DataStaxAstraConfig.class)
@SpringBootApplication
public class BetterreadsDataLoaderApplication {

    public static void main(String[] args) {
        SpringApplication.run(BetterreadsDataLoaderApplication.class, args);
    }

    /**
     * This is necessary to have the Spring Boot app use the Astra secure bundle
     * to connect to the database
     */
    @Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraConfig astraProperties) {
        Path bundle = astraProperties.getSecureConnectBundle().toPath();
        return builder -> builder.withCloudSecureConnectBundle(bundle);
    }
}
