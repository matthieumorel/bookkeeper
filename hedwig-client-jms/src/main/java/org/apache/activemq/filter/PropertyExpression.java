/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.filter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;

import org.apache.derby.iapi.store.raw.xact.TransactionId;
import org.apache.hedwig.jms.administered.HedwigDestination;
import org.apache.hedwig.jms.message.HedwigJMSMessage;
import org.apache.hedwig.jms.util.JMSUtils;

/**
 * Represents a property expression
 * 
 * 
 */
public class PropertyExpression implements Expression {

    private static final Map<String, SubExpression> JMS_PROPERTY_EXPRESSIONS = new HashMap<String, SubExpression>();

    interface SubExpression {
        Object evaluate(HedwigJMSMessage message) throws JMSException;
    }

    static {
        JMS_PROPERTY_EXPRESSIONS.put("JMSDestination", new SubExpression() {

            public Object evaluate(HedwigJMSMessage message) throws JMSException {
                HedwigDestination dest = message.getOriginalJMSDestination();
                if (dest == null) {
                    dest = (HedwigDestination) message.getJMSDestination();
                }
                if (dest == null) {
                    return null;
                }
                return dest.toString();
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSReplyTo", new SubExpression() {

            public Object evaluate(HedwigJMSMessage message) throws JMSException {
                if (message.getJMSReplyTo() == null) {
                    return null;
                }
                return message.getJMSReplyTo().toString();
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSType", new SubExpression() {

            public Object evaluate(HedwigJMSMessage message) throws JMSException {
                return message.getJMSType();
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSDeliveryMode", new SubExpression() {

            public Object evaluate(HedwigJMSMessage message) {
                return Integer.valueOf(message.isPersistent() ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSPriority", new SubExpression() {

            public Object evaluate(HedwigJMSMessage message) throws JMSException {
                return Integer.valueOf(message.getJMSPriority());
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSHedwigJMSMessageID", new SubExpression() {

            public Object evaluate(HedwigJMSMessage message) throws JMSException {
                if (message.getJMSMessageID() == null) {
                    return null;
                }
                return message.getJMSMessageID().toString();
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSTimestamp", new SubExpression() {

            public Object evaluate(HedwigJMSMessage message) throws JMSException {
                return Long.valueOf(message.getJMSTimestamp());
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSCorrelationID", new SubExpression() {

            public Object evaluate(HedwigJMSMessage message) throws JMSException {
                return message.getJMSCorrelationID();
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSExpiration", new SubExpression() {

            public Object evaluate(HedwigJMSMessage message) throws JMSException {
                return Long.valueOf(message.getJMSExpiration());
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSRedelivered", new SubExpression() {

            public Object evaluate(HedwigJMSMessage message) throws JMSException {
                return Boolean.valueOf(message.getJMSRedelivered());
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSXDeliveryCount", new SubExpression() {

            public Object evaluate(HedwigJMSMessage message) {
                return Integer.valueOf(message.getRedeliveryCounter() + 1);
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSXGroupID", new SubExpression() {

            public Object evaluate(HedwigJMSMessage message) {
                return message.getGroupID();
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSXGroupSeq", new SubExpression() {

            public Object evaluate(HedwigJMSMessage message) {
                return new Integer(message.getGroupSequence());
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSXProducerTXID", new SubExpression() {

            public Object evaluate(HedwigJMSMessage message) {
                TransactionId txId = message.getOriginalTransactionId();
                if (txId == null) {
                    txId = message.getTransactionId();
                }
                if (txId == null) {
                    return null;
                }
                return new Integer(txId.toString());
            }
        });

    }

    private final String name;
    private final SubExpression jmsPropertyExpression;

    public PropertyExpression(String name) {
        this.name = name;
        jmsPropertyExpression = JMS_PROPERTY_EXPRESSIONS.get(name);
    }

    public Object evaluate(MessageEvaluationContext message) throws JMSException {
        try {
            if (message.isDropped()) {
                return null;
            }

            if (jmsPropertyExpression != null) {
                return jmsPropertyExpression.evaluate(message.getMessage());
            }
            try {
                return message.getMessage().getObjectProperty(name);
            } catch (IOException ioe) {
                throw JMSUtils.createJMSException("Could not get property: " + name + " reason: " + ioe.getMessage(),
                        ioe);
            }
        } catch (IOException e) {
            throw JMSUtils.createJMSException(e);
        }

    }

    public Object evaluate(HedwigJMSMessage message) throws JMSException {
        if (jmsPropertyExpression != null) {
            return jmsPropertyExpression.evaluate(message);
        }
        return message.getObjectProperty(name);
    }

    public String getName() {
        return name;
    }

    /**
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return name;
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        return name.hashCode();
    }

    /**
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public boolean equals(Object o) {

        if (o == null || !this.getClass().equals(o.getClass())) {
            return false;
        }
        return name.equals(((PropertyExpression) o).name);

    }

}
