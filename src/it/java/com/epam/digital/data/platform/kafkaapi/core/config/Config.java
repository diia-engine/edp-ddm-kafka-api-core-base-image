package com.epam.digital.data.platform.kafkaapi.core.config;

import com.epam.digital.data.platform.integration.idm.model.PublishedIdmRealm;
import com.epam.digital.data.platform.integration.idm.service.PublicIdmService;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

import static org.mockito.ArgumentMatchers.any;

@Configuration
@Profile("int-test")
public class Config {
    @Bean
    @Primary
    public PublicIdmService publicIdmServiceTest() {
        PublicIdmService publicIdmService = Mockito.mock(PublicIdmService.class);
        Mockito.when(publicIdmService.getRealm(any())).thenReturn(Mockito.mock(PublishedIdmRealm.class));
        return publicIdmService;
    }
}
