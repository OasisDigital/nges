package com.oasisdigital.nges.event;

import java.time.OffsetDateTime;

public class Lease {
    private String leaseKey;
    private String ownerKey;
    private OffsetDateTime expirationDate;

    public String getLeaseKey() {
        return leaseKey;
    }

    public void setLeaseKey(String leaseKey) {
        this.leaseKey = leaseKey;
    }

    public String getOwnerKey() {
        return ownerKey;
    }

    public void setOwnerKey(String ownerKey) {
        this.ownerKey = ownerKey;
    }

    public OffsetDateTime getExpirationDate() {
        return expirationDate;
    }

    public void setExpirationDate(OffsetDateTime expirationDate) {
        this.expirationDate = expirationDate;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((expirationDate == null) ? 0 : expirationDate.hashCode());
        result = prime * result + ((leaseKey == null) ? 0 : leaseKey.hashCode());
        result = prime * result + ((ownerKey == null) ? 0 : ownerKey.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Lease other = (Lease) obj;
        if (expirationDate == null) {
            if (other.expirationDate != null)
                return false;
        } else if (!expirationDate.equals(other.expirationDate))
            return false;
        if (leaseKey == null) {
            if (other.leaseKey != null)
                return false;
        } else if (!leaseKey.equals(other.leaseKey))
            return false;
        if (ownerKey == null) {
            if (other.ownerKey != null)
                return false;
        } else if (!ownerKey.equals(other.ownerKey))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return leaseKey + ":" + ownerKey + ":" + expirationDate;
    }

    public boolean isOwnedBy(String owner) {
        return getOwnerKey().equals(owner);
    }
}
