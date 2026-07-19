package com.bankstream.fraud.model;

public enum AccountTier {
    STANDARD,
    PREMIUM,
    ELITE;

    public double getHighValueThreshold() {
        return switch (this) {
            case STANDARD -> 50_000.0;
            case PREMIUM -> 1_00_000.0;
            case ELITE -> 5_00_000.0;
        };
    }

    public static AccountTier fromString(String value) {
        try {
            return AccountTier.valueOf(value.toUpperCase());
        } catch (Exception e) {
            return STANDARD;
        }
    }
}
