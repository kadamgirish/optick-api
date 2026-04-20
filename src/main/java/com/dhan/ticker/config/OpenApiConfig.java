package com.dhan.ticker.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Dhan Tick Logger API")
                        .version("1.0.0")
                        .description("Connect to Dhan WebSocket, select an index, and log live tick data")
                        .contact(new Contact().name("Developer")));
    }
}
