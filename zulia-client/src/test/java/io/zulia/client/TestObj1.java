package io.zulia.client;

import io.zulia.fields.annotations.UniqueId;

import java.util.Date;
import java.util.List;
import java.util.Set;

public class TestObj1 {

    @UniqueId
    private String field1;
    private int field2;
    private List<String> field3;
    private Set<Integer> field4;
    private Date field5;
    private long field6;
    private boolean field7;

    public TestObj1(String field1, int field2, List<String> field3, Set<Integer> field4, Date field5, long field6, boolean field7) {
        this.field1 = field1;
        this.field2 = field2;
        this.field3 = field3;
        this.field4 = field4;
        this.field5 = field5;
        this.field6 = field6;
        this.field7 = field7;
    }

    public String getField1() {
        return field1;
    }

    public void setField1(String field1) {
        this.field1 = field1;
    }

    public int getField2() {
        return field2;
    }

    public void setField2(int field2) {
        this.field2 = field2;
    }

    public List<String> getField3() {
        return field3;
    }

    public void setField3(List<String> field3) {
        this.field3 = field3;
    }

    public Set<Integer> getField4() {
        return field4;
    }

    public void setField4(Set<Integer> field4) {
        this.field4 = field4;
    }

    public Date getField5() {
        return field5;
    }

    public void setField5(Date field5) {
        this.field5 = field5;
    }

    public long getField6() {
        return field6;
    }

    public void setField6(long field6) {
        this.field6 = field6;
    }

    public boolean isField7() {
        return field7;
    }

    public void setField7(boolean field7) {
        this.field7 = field7;
    }

    @Override
    public String toString() {
        return "TestObj1{" + "field1='" + field1 + '\'' + ", field2=" + field2 + ", field3=" + field3 + ", field4=" + field4 + '}';
    }
}
