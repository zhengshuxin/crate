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

package io.crate.operation.scalar;

import com.google.common.collect.ImmutableList;
import io.crate.TimestampFormat;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.TimestampType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.BytesRefs;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.List;
import java.util.Locale;

public class DateFormatFunction extends Scalar<BytesRef, Object> {

    public static final String NAME = "date_format";
    public static final DateTimeFormatter DEFAULT_FORMATTER = TimestampFormat.DATE_TIME_FORMATTER.printer();

    public static void register(ScalarFunctionModule module) {
        List<DataType> supportedTimestampTypes = ImmutableList.<DataType>of(
                DataTypes.TIMESTAMP, DataTypes.LONG, DataTypes.STRING);
        for (DataType dataType : supportedTimestampTypes) {
            // without format
            module.register(new DateFormatFunction(new FunctionInfo(
                    new FunctionIdent(NAME, ImmutableList.of(dataType)),
                    DataTypes.STRING)
            ));
            // with format
            module.register(new DateFormatFunction(new FunctionInfo(
                    new FunctionIdent(NAME, ImmutableList.of(DataTypes.STRING, dataType)),
                    DataTypes.STRING)
            ));
            // time zone aware variant
            module.register(new DateFormatFunction(new FunctionInfo(
                    new FunctionIdent(NAME, ImmutableList.of(DataTypes.STRING, DataTypes.STRING, dataType)),
                    DataTypes.STRING)
            ));
        }
    }

    private FunctionInfo info;
    @Nullable
    private DateTimeFormatter formatter;


    public DateFormatFunction(FunctionInfo info) {
        this.info = info;
    }

    public DateFormatFunction(FunctionInfo info, org.joda.time.format.DateTimeFormatter formatter) {
        this(info);
        this.formatter = formatter;
    }

    @Override
    public BytesRef evaluate(Input<Object>... args) {
        if (hasNullInputs(args)) {
            return null;
        }
        DateTimeFormatter evalFormatter = formatter;
        if (evalFormatter == null) {
            if (args.length == 1) {
                evalFormatter = DEFAULT_FORMATTER;
            } else {
                evalFormatter = getFormatter(
                        args[0],
                        args.length == 3 ? args[1] : null
                );
            }
        }
        Object tsValue = args[args.length-1].value();
        Long timestamp = TimestampType.INSTANCE.value(tsValue);
        return new BytesRef(evalFormatter.print(timestamp));
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol) {
        assert symbol != null;
        assert symbol.arguments().size() > 0 && symbol.arguments().size() < 4 : "invalid number of arguments";

        return evaluateIfLiterals(this, symbol);
    }

    @Override
    public Scalar<BytesRef, Object> compile(List<Symbol> arguments) {
        if (!containsNullLiteral(arguments)) {
            Symbol firstArg = arguments.get(0);
            switch (arguments.size()) {
                // case 1 - does not need to be compiled
                case 2:
                    if (firstArg.isLiteral()) {
                        return new DateFormatFunction(
                                info,
                                getFormatter((Literal)firstArg, null)
                        );
                    }
                    break;

                case 3:
                    if (firstArg.isLiteral() && arguments.get(1).isLiteral()) {
                        return new DateFormatFunction(
                                info,
                                getFormatter((Literal)firstArg, (Literal)arguments.get(1))
                        );
                    }
                    break;
            }
        }
        // not compilable
        return this;
    }

    private static DateTimeFormatter getFormatter(Input<?> formatLiteral, @Nullable Input<?> timezoneLiteral) {
        assert formatLiteral.value() != null : "NULL format for DateFormat Function";
        String formatString = BytesRefs.toString(formatLiteral.value());
        DateTimeZone timezone = DateTimeZone.UTC;
        if (timezoneLiteral != null) {
            timezone = TimeZoneParser.parseTimeZone(
                    BytesRefs.toBytesRef(timezoneLiteral.value()));
        }
        return DateTimeFormat.forPattern(formatString)
                .withChronology(ISOChronology.getInstance(timezone))
                .withLocale(Locale.ENGLISH); // TODO: chose another one? Locale.ROOT ? use configured server Locale?
    }
}
