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
                        .title("Optick API")
                        .version("1.0.0")
                        .description("Live options tick data, OI tracking, and multi-index subscription via Dhan WebSocket")
                        .contact(new Contact().name("Developer")));
    }
}
