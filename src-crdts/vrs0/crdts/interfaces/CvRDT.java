package vrs0.crdts.interfaces;

import java.io.Serializable;

import vrs0.clocks.CausalityClock;
import vrs0.exceptions.IncompatibleTypeException;

public interface CvRDT extends Serializable {
    void merge(CvRDT other, CausalityClock thisClock, CausalityClock thatClock)
            throws IncompatibleTypeException;
    
    boolean equals(CvRDT o);
}
