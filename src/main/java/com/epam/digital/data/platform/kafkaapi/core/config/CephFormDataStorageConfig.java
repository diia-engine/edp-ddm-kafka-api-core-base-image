/*
 * Copyright 2023 EPAM Systems.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.epam.digital.data.platform.kafkaapi.core.config;

import com.epam.digital.data.platform.integration.ceph.factory.CephS3Factory;
import com.epam.digital.data.platform.storage.file.config.FileDataCephStorageConfiguration;
import com.epam.digital.data.platform.storage.file.factory.FormDataFileStorageServiceFactory;
import com.epam.digital.data.platform.storage.file.service.FormDataFileKeyProvider;
import com.epam.digital.data.platform.storage.file.service.FormDataFileKeyProviderImpl;
import com.epam.digital.data.platform.storage.file.service.FormDataFileStorageService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CephFormDataStorageConfig {

    @Bean
    public FormDataFileKeyProvider formDataFileKeyProvider() {
        return new FormDataFileKeyProviderImpl();
    }

    @Bean
    public FormDataFileStorageServiceFactory fileStorageFactory(CephS3Factory cephS3Factory) {
        return new FormDataFileStorageServiceFactory(cephS3Factory);
    }

    @Bean
    @ConditionalOnProperty(prefix = "storage.lowcode-file-storage", name = "type", havingValue = "ceph")
    @ConfigurationProperties(prefix = "storage.lowcode-file-storage.backend.ceph")
    public FileDataCephStorageConfiguration lowcodeCephFileDataStorageConfiguration() {
        return new FileDataCephStorageConfiguration();
    }

    @Bean
    @ConditionalOnProperty(prefix = "storage.datafactory-file-storage", name = "type", havingValue = "ceph")
    @ConfigurationProperties(prefix = "storage.datafactory-file-storage.backend.ceph")
    public FileDataCephStorageConfiguration datafactoryCephFileDataStorageConfiguration() {
        return new FileDataCephStorageConfiguration();
    }

    @Bean
    @ConditionalOnBean(name = "lowcodeCephFileDataStorageConfiguration")
    public FormDataFileStorageService lowcodeFileDataStorageService(
            FormDataFileStorageServiceFactory fileStorageFactory,
            FileDataCephStorageConfiguration lowcodeCephFileDataStorageConfiguration) {
        return fileStorageFactory.fromDataFileStorageService(
                lowcodeCephFileDataStorageConfiguration);
    }

    @Bean
    @ConditionalOnBean(name = "datafactoryCephFileDataStorageConfiguration")
    public FormDataFileStorageService datafactoryFileDataStorageService(
            FormDataFileStorageServiceFactory fileStorageFactory,
            FileDataCephStorageConfiguration datafactoryCephFileDataStorageConfiguration) {
        return fileStorageFactory.fromDataFileStorageService(
                datafactoryCephFileDataStorageConfiguration);
    }
}
