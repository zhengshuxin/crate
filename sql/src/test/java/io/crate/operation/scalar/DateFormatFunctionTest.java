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
import com.google.common.collect.Lists;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.crate.testing.TestingHelpers.*;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class DateFormatFunctionTest extends AbstractScalarFunctionsTest {

    Iterable<Literal> timestampEquivalents(String ts) {
        Literal tsLiteral = Literal.newLiteral(DataTypes.TIMESTAMP, DataTypes.TIMESTAMP.value(ts));
        return ImmutableList.of(
                tsLiteral,
                Literal.convert(tsLiteral, DataTypes.LONG),
                Literal.newLiteral(ts)
        );
    }

    public Symbol normalizeForArgs(List<Symbol> args) {
        Function function = createFunction(DateFormatFunction.NAME, DataTypes.STRING, args);
        FunctionImplementation impl = functions.get(function.info().ident());
        if (randomBoolean()) {
            impl = ((Scalar)impl).compile(function.arguments());
        }

        return impl.normalizeSymbol(function);
    }
    
    public Object evaluateForArgs(List<Symbol> args) {
        Function function = createFunction(DateFormatFunction.NAME, DataTypes.STRING, args);
        Scalar impl = (Scalar)functions.get(function.info().ident());
        if (randomBoolean()) {
            impl = impl.compile(function.arguments());
        }
        Input[] inputs = new Input[args.size()];
        for (int i = 0; i < args.size(); i++) {
            inputs[i] = (Input)args.get(i);
        }
        return impl.evaluate(inputs);
    }

    @Test
    public void testNormalizeDefault() throws Exception {
        for (Literal tsLiteral : timestampEquivalents("1970-01-01T00:00:00")) {
            List<Symbol> args = Lists.<Symbol>newArrayList(
                    tsLiteral
            );
            assertThat(
                    normalizeForArgs(args),
                    isLiteral(new BytesRef("1970-01-01T00:00:00.000Z")));
        }
    }

    @Test
    public void testNormalizeDefaultTimezone() throws Exception {
        for (Literal tsLiteral : timestampEquivalents("1970-02-01")) {
            List<Symbol> args = Lists.<Symbol>newArrayList(
                    Literal.newLiteral("dd.MM.yyyy ZZZ"),
                    tsLiteral
            );
            assertThat(
                    normalizeForArgs(args),
                    isLiteral(new BytesRef("01.02.1970 UTC")));
        }
    }

    @Test
    public void testNormalizeWithTimezone() throws Exception {
        for (Literal tsLiteral : timestampEquivalents("1970-01-01T00:00:00")) {
            List<Symbol> args = Lists.<Symbol>newArrayList(
                    Literal.newLiteral("dd.MM.yyyy HH:mm:ss ZZZ"),
                    Literal.newLiteral("Europe/Rome"),
                    tsLiteral
            );
            assertThat(
                    normalizeForArgs(args),
                    isLiteral(new BytesRef("01.01.1970 01:00:00 Europe/Rome")));
        }
    }

    @Test
    public void testNormalizeReferenceWithTimezone() throws Exception {
        for (Literal tsLiteral : timestampEquivalents("1970-01-01T00:00:00")) {
            List<Symbol> args = Lists.newArrayList(
                    Literal.newLiteral("dd.MM.yyyy HH:mm:ss ZZZ"),
                    createReference("timezone", DataTypes.STRING),
                    tsLiteral
            );
            Function function = createFunction(DateFormatFunction.NAME, DataTypes.STRING, args);

            FunctionImplementation dateFormat = functions.get(function.info().ident());
            if (randomBoolean()) {
                dateFormat = ((Scalar)dateFormat).compile(args);
            }
            Symbol normalized = dateFormat.normalizeSymbol(function);

            assertSame(function, normalized);
        }
    }

    @Test
    public void testNormalizeWithNullLiteral() throws Exception {
        Literal timestampNull = Literal.newLiteral(DataTypes.TIMESTAMP, null);
        List<List<Symbol>> argLists = Arrays.asList(
                Arrays.<Symbol>asList(
                        Literal.newLiteral("dd.MM.yyyy HH:mm:ss ZZZ"),
                        Literal.newLiteral(DataTypes.STRING, null),
                        timestampNull
                ),
                Arrays.<Symbol>asList(
                        Literal.newLiteral(DataTypes.STRING, null),
                        Literal.newLiteral("Europe/Berlin"),
                        Literal.newLiteral(DataTypes.TIMESTAMP, 0L)
                ),
                Arrays.<Symbol>asList(timestampNull),
                Arrays.<Symbol>asList(Literal.newLiteral(DataTypes.STRING, null), timestampNull)
        );
        for (List<Symbol> argList : argLists) {
            assertThat(normalizeForArgs(argList), isLiteral(null));
        }
    }

    @Test
    public void testEvaluateDefault() throws Exception {
        for (Literal tsLiteral : timestampEquivalents("2015-06-10T09:03:00.004+02")) {

            List<Symbol> args = Arrays.<Symbol>asList(
                    tsLiteral
            );
            Object value = evaluateForArgs(args);
            assertThat(value, instanceOf(BytesRef.class));
            assertThat(((BytesRef)value).utf8ToString(), is("2015-06-10T07:03:00.004Z"));
        }
    }

    @Test
    public void testEvaluateDefaultTimezone() throws Exception {
        for (Literal tsLiteral : timestampEquivalents("2055-01-01")) {

            List<Symbol> args = Arrays.<Symbol>asList(
                    Literal.newLiteral("G C Y x w e E y D M d a K h H k m s S z Z"),
                    tsLiteral
            );
            Object value = evaluateForArgs(args);
            assertThat(value, instanceOf(BytesRef.class));
            assertThat(((BytesRef)value).utf8ToString(), is("AD 20 2055 2054 53 5 Fri 2055 1 1 1 AM 0 12 0 24 0 0 0 UTC +0000"));
        }
    }

    @Test
    public void testEvaluateWithTimezone() throws Exception {
        for (Literal tsLiteral : timestampEquivalents("1871-01-01T09:00:00.000Z")) {

            List<Symbol> args = Arrays.<Symbol>asList(
                    Literal.newLiteral("GGGG CCCC YYYY xxxx wwww eeee EEEE yyyy DDDD MMMM dddd aaaa KKKK hhhh HHHH kkkk mmmm ssss SSSS zzzz ZZZZ"),
                    Literal.newLiteral("EST"),
                    tsLiteral
            );
            Object value = evaluateForArgs(args);
            assertThat(value, instanceOf(BytesRef.class));
            assertThat(((BytesRef)value).utf8ToString(), is("AD 0018 1871 1870 0052 0007 Sunday 1871 0001 January 0001 AM 0004 0004 0004 0004 0000 0000 0000 Eastern Standard Time EST"));
        }
    }

    @Test
    public void testEvaluateWithNullInputs() throws Exception {
        Literal timestampNull = Literal.newLiteral(DataTypes.TIMESTAMP, null);
        List<List<Symbol>> argLists = Arrays.asList(
                Arrays.<Symbol>asList(
                        Literal.newLiteral("dd.MM.yyyy HH:mm:ss ZZZ"),
                        Literal.newLiteral(DataTypes.STRING, null),
                        timestampNull
                ),
                Arrays.<Symbol>asList(
                        Literal.newLiteral(DataTypes.STRING, null),
                        Literal.newLiteral("Europe/Berlin"),
                        Literal.newLiteral(DataTypes.TIMESTAMP, 0L)
                ),
                Arrays.<Symbol>asList(timestampNull),
                Arrays.<Symbol>asList(Literal.newLiteral(DataTypes.STRING, null), timestampNull)
        );
        for (List<Symbol> argList : argLists) {
            Object value = evaluateForArgs(argList);
            assertThat(value, is(nullValue()));
        }
    }

    @Test
    public void testNormalizeInvalidFormat() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Illegal pattern component: X");
        List<Symbol> args = Arrays.<Symbol>asList(
                Literal.newLiteral("XYZ"),
                Literal.newLiteral(0L)
        );
        normalizeForArgs(args);
    }

    @Test
    public void testEvaluateInvalidFormat() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Illegal pattern component: X");
        List<Symbol> args = Arrays.<Symbol>asList(
                Literal.newLiteral("XYZ"),
                Literal.newLiteral(0L)
        );
        evaluateForArgs(args);
    }

    @Test
    public void testNormalizeInvalidTimeZone() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid time zone value 'wrong timezone'");
        List<Symbol> args = Arrays.<Symbol>asList(
                Literal.newLiteral("HH:MM:ss"),
                Literal.newLiteral("wrong timezone"),
                Literal.newLiteral(0L)
        );
        normalizeForArgs(args);
    }

    @Test
    public void testEvaluateInvalidTimeZone() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid time zone value 'wrong timezone'");
        List<Symbol> args = Arrays.<Symbol>asList(
                Literal.newLiteral("HH:MM:ss"),
                Literal.newLiteral("wrong timezone"),
                Literal.newLiteral(0L)
        );
        evaluateForArgs(args);
    }

    @Test
    public void testNormalizeInvalidTimestamp() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid format: \"NO TIMESTAMP\"");
        List<Symbol> args = Arrays.<Symbol>asList(
                Literal.newLiteral("HH:MM:ss"),
                Literal.newLiteral("NO TIMESTAMP")
        );
        normalizeForArgs(args);

    }

    @Test
    public void testEvaluateInvalidTimestamp() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid format: \"NO TIMESTAMP\"");
        List<Symbol> args = Arrays.<Symbol>asList(
                Literal.newLiteral("HH:MM:ss"),
                Literal.newLiteral("NO TIMESTAMP")
        );
        evaluateForArgs(args);

    }
}
