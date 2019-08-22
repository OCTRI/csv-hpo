package org.octri.csvhpo.domain;

public class LabHpo {

    int rowid;
    String negated;
    String mapTo;

    public LabHpo(int rowid, String negated, String mapTo) {
        this.rowid = rowid;
        this.negated = negated;
        this.mapTo = mapTo;
    }

    public int getRowid() {
        return rowid;
    }

    public void setRowid(int rowid) {
        this.rowid = rowid;
    }

    public String getNegated() {
        return negated;
    }

    public void setNegated(String negated) {
        this.negated = negated;
    }

    public String getMapTo() {
        return mapTo;
    }

    public void setMapTo(String mapTo) {
        this.mapTo = mapTo;
    }

    @Override
    public String toString(){
        return String.format("%d, %s, %s", this.rowid, this.negated, this.mapTo);
    }
}
