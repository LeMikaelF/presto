/*
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
package com.facebook.presto.kafka.schema.file;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.decoder.dummy.DummyRowDecoder;
import com.facebook.presto.kafka.KafkaConnectorConfig;
import com.facebook.presto.kafka.KafkaTopicDescription;
import com.facebook.presto.kafka.KafkaTopicFieldGroup;
import com.facebook.presto.kafka.schema.MapBasedTableDescriptionSupplier;
import com.facebook.presto.kafka.schema.TableDescriptionSupplier;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;
import javax.inject.Provider;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.nio.file.Files.readAllBytes;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class FileTableDescriptionSupplier
        implements Provider<TableDescriptionSupplier>
{
    public static final String NAME = "file";

    private static final Logger log = Logger.get(FileTableDescriptionSupplier.class);

    private final JsonCodec<KafkaTopicDescription> topicDescriptionCodec;
    private final File tableDescriptionDir;
    private final SchemaTableNameFactory tableNameFactory;
    private final Set<String> tableNames;

    @Inject
    FileTableDescriptionSupplier(FileTableDescriptionSupplierConfig config, KafkaConnectorConfig kafkaConnectorConfig,
            JsonCodec<KafkaTopicDescription> topicDescriptionCodec)
    {
        this.topicDescriptionCodec = requireNonNull(topicDescriptionCodec, "topicDescriptionCodec is null");
        requireNonNull(config, "config is null");
        requireNonNull(kafkaConnectorConfig, "kafkaConfig is null");
        this.tableDescriptionDir = config.getTableDescriptionDir();
        this.tableNameFactory = new SchemaTableNameFactory(kafkaConnectorConfig.getDefaultSchema());
        this.tableNames = ImmutableSet.copyOf(config.getTableNames());
    }

    @Override
    public TableDescriptionSupplier get()
    {
        Map<SchemaTableName, KafkaTopicDescription> tables = populateTables();
        return new MapBasedTableDescriptionSupplier(tables);
    }

    private Map<SchemaTableName, KafkaTopicDescription> populateTables()
    {
        ImmutableMap.Builder<SchemaTableName, KafkaTopicDescription> builder = ImmutableMap.builder();

        log.debug("Loading kafka table definitions from %s", tableDescriptionDir.getAbsolutePath());

        try {
            for (File file : listFiles(tableDescriptionDir)) {
                if (file.isFile() && file.getName().endsWith(".json")) {
                    KafkaTopicDescription table = topicDescriptionCodec.fromJson(readAllBytes(file.toPath()));
                    SchemaTableName schemaTableName = tableNameFactory.create(table.getSchemaName().orElse(null), table.getTableName());
                    log.debug("Kafka table %s: %s", schemaTableName, table);
                    builder.put(schemaTableName, table);
                }
            }

            Map<SchemaTableName, KafkaTopicDescription> tableDefinitions = builder.build();

            log.debug("Loaded Table definitions: %s", tableDefinitions.keySet());

            builder = ImmutableMap.builder();
            for (String definedTable : tableNames) {
                SchemaTableName schemaTableName = this.tableNameFactory.create(definedTable);

                if (tableDefinitions.containsKey(schemaTableName)) {
                    KafkaTopicDescription kafkaTable = tableDefinitions.get(schemaTableName);
                    log.debug("Found Table definition for %s: %s", schemaTableName, kafkaTable);
                    builder.put(schemaTableName, kafkaTable);
                }
                else {
                    // A dummy table definition only supports the internal columns.
                    log.debug("Created dummy Table definition for %s", schemaTableName);

                    builder.put(schemaTableName, new KafkaTopicDescription(
                            schemaTableName.getTableName(),
                            Optional.of(schemaTableName.getSchemaName()),
                            schemaTableName.getTableName(),
                            Optional.of(new KafkaTopicFieldGroup(DummyRowDecoder.NAME, Optional.empty(), ImmutableList.of())),
                            Optional.of(new KafkaTopicFieldGroup(DummyRowDecoder.NAME, Optional.empty(), ImmutableList.of()))));
                }
            }

            return builder.build();
        }
        catch (IOException e) {
            log.warn(e, "Error: ");
            throw new UncheckedIOException(e);
        }
    }

    private static List<File> listFiles(File dir)
    {
        if ((dir != null) && dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                log.debug("Considering files: %s", asList(files));
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }
}
