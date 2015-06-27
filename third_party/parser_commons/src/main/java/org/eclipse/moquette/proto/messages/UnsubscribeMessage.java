/*
 * Copyright (c) 2012-2014 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */
package org.eclipse.moquette.proto.messages;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author andrea
 */
public class UnsubscribeMessage extends MessageIDMessage {
    List<String> m_types = new ArrayList<String>();
    
    public UnsubscribeMessage() {
        m_messageType = AbstractMessage.UNSUBSCRIBE;
    }

    public List<String> topicFilters() {
        return m_types;
    }

    public void addTopicFilter(String type) {
        m_types.add(type);
    }
}
