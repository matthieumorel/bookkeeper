package org.apache.hedwig.jms.administered;

import javax.jms.Destination;

public class HedwigDestination implements Destination {

    public boolean isComposite() {
        // TODO Auto-generated method stub
        return false;
    }

    public HedwigDestination[] getCompositeDestinations() {
        // TODO Auto-generated method stub
        return null;
    }

    public String[] getDestinationPaths() {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean isTemporary() {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean isQueue() {
        // TODO Auto-generated method stub
        return false;
    }

    public static int compare(HedwigDestination destination, HedwigDestination destination2) {
        // TODO Auto-generated method stub
        return 0;
    }

    public byte getDestinationType() {
        // TODO Auto-generated method stub
        return 0;
    }

    public String getPhysicalName() {
        // TODO Auto-generated method stub
        return null;
    }

}
