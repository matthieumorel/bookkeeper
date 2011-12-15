package org.apache.hedwig.jms.administered;

import javax.jms.Destination;

public abstract class HedwigDestination implements Destination {
    public static final byte TEMPORARY_TOPIC_DEST_TYPE = 1;
    public static final byte TOPIC_DEST_TYPE = 2;
    public static final byte QUEUE_DEST_TYPE = 2;

    public abstract byte getDestinationType();

    public boolean isComposite() {
        // TODO Auto-generated method stub
        return false;
    }

    public HedwigDestination[] getCompositeDestinations() {
        throw new RuntimeException("Not implemented yet");
    }

    public static int compare(HedwigDestination destination, HedwigDestination destination2) {
        throw new RuntimeException("Not implemented yet");
    }

    public abstract String[] getDestinationPaths();

    public abstract boolean isTemporary();

    public abstract boolean isQueue();

    public abstract String getPhysicalName();

}
