/*
 * Copyright (c) 2020 Simon Johnson <simon622 AT gmail DOT com>
 *
 * Find me on GitHub:
 * https://github.com/simon622
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.slj.mqtt.sn.impl;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.MessageFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class MqttsnLogFormatter extends Formatter {

    private final static char SQUARE_O = '[';
    private final static char SQUARE_C = ']';
    private final static char WHITESPACE = ' ';
    private final static String format = "{0,date} {0,time}";
    private MessageFormat formatter = new MessageFormat(format);
    private Object args[] = new Object[1];

    private String lineSeparator = System.lineSeparator();

    public String format(LogRecord record) {

        StringBuilder sb = new StringBuilder();
        args[0] = new Date(record.getMillis());

        //-- date time
        StringBuffer text = new StringBuffer(SQUARE_O);
        synchronized (formatter){
            formatter.format(args, text, null);
        }
        sb.append(text);
        sb.append(SQUARE_C);
        sb.append(WHITESPACE);

        //-- Class name
        if (record.getSourceClassName() != null) {
            sb.append(record.getSourceClassName());
        } else {
            sb.append(record.getLoggerName());
        }

        sb.append(" - "); // lineSeparator

        String message = formatMessage(record);

        //-- level
        sb.append(record.getLevel().getLocalizedName());
        sb.append(": ");
        int iOffset = (1000 - record.getLevel().intValue()) / 100;
        for( int i = 0; i < iOffset;  i++ ){
            sb.append(WHITESPACE);
        }

        sb.append(message);
        sb.append(lineSeparator);
        if (record.getThrown() != null) {
            try {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                record.getThrown().printStackTrace(pw);
                pw.close();
                sb.append(sw.toString());
            } catch (Exception ex) {
            }
        }
        return sb.toString();
    }
}
