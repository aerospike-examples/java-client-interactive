package com.aerospike.timf.example;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class AddressGeneratorService {
    private String[] US_STATE_ABBREVIATIONS = new String[] {
        "AL", "AK", "AZ", "AR","AS", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "GU", "HI", "ID", "IL", "IN", "IA", "KS",
        "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "MP", 
        "OH", "OK", "OR", "PA", "PR", "RI", "SC", "SD", "TN", "TX", "TT", "UT", "VT", "VA", "VI", "WA", "WV", "WI", "WY"
    };
    
    private NameGeneratorService ns = new NameGeneratorService();
    
    public String getAddressLine1() {
        Random random = ThreadLocalRandom.current();
        return String.format("%d %s St", (random.nextInt(1000)+1)*10, ns.getLastName()); 
    }
    
    public String getSuburb() {
        return ns.getLastName();
    }
    public String getState() {
        Random random = ThreadLocalRandom.current();
        return US_STATE_ABBREVIATIONS[random.nextInt(US_STATE_ABBREVIATIONS.length)];
    }
    
    public String getZipCode() {
        Random random = ThreadLocalRandom.current();
        return "" + 10000 + random.nextInt(9000);
    }
}
