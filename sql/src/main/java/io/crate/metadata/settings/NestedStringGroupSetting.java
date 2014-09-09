/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.metadata.settings;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.types.DataTypes;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class NestedStringGroupSetting extends NestedSetting {

    private List<Setting> children = new ArrayList<>();

    @Override
    public Object extract(Settings settings) {
        Map<String, String> settingsMap = settings.getAsSettings(settingName()).getAsMap();
        if (!settingsMap.isEmpty()) {
            final Setting that = this;
            for (final Map.Entry<String, String> entry : settingsMap.entrySet()) {
                Setting setting = new StringSetting() {
                    @Override
                    public String name() {
                        return entry.getKey();
                    }

                    @Override
                    public Setting parent() {
                        return that;
                    }
                };
                if (!children.contains(setting)) {
                    children.add(setting);
                    ImmutableList.Builder<String> builder = ImmutableList.builder();
                    builder.addAll(chain());
                    builder.add(entry.getKey());
                    SysClusterTableInfo.register("settings", DataTypes.STRING, builder.build());
                }
            }
        }

        return settingsMap;
    }

    @Override
    public List<Setting> children() {
        return children;
    }
}