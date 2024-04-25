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

import com.facebook.presto.spi.SchemaTableName;

import javax.annotation.Nullable;

import static org.apache.bval.util.ObjectUtils.defaultIfNull;

class SchemaTableNameFactory
{

    private final String defaultSchema;

    SchemaTableNameFactory(String defaultSchema)
    {
        this.defaultSchema = defaultSchema;
    }

    public SchemaTableName create(@Nullable String schema, String table)
    {
        return new SchemaTableName(defaultIfNull(schema, defaultSchema), table);
    }

    public SchemaTableName create(String schemaTableName)
    {
        String[] parts = schemaTableName.split("\\.", 2);

        if (parts.length == 0) {
            throw new IllegalArgumentException("Table name should not be empty");
        }

        if (parts.length == 1) {
            return new SchemaTableName(defaultSchema, parts[0]);
        }

        return new SchemaTableName(parts[0], parts[1]);
    }
}
