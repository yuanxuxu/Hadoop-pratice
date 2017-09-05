package Join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import org.apache.hadoop.io.WritableComparable;

public class EmpJoinDep implements WritableComparable{

    private String Name="";
    private String Sex="";
    private int Age=0;
    private int DepNo=0;
    private String DepName="";
    private String table="";
    public EmpJoinDep() {}

    public EmpJoinDep(EmpJoinDep empJoinDep) {
        this.Name = empJoinDep.getName();
        this.Sex = empJoinDep.getSex();
        this.Age = empJoinDep.getAge();
        this.DepNo = empJoinDep.getDepNo();
        this.DepName = empJoinDep.getDepName();
        this.table = empJoinDep.getTable();
    }

    public String getName() {
        return Name;
    }

    public void setName(String name) {
        Name = name;
    }

    public String getSex() {
        return Sex;
    }

    public void setSex(String sex) {
        this.Sex = sex;
    }

    public int getAge() {
        return Age;
    }

    public void setAge(int age) {
        this.Age = age;
    }

    public int getDepNo() {
        return DepNo;
    }

    public void setDepNo(int depNo) {
        DepNo = depNo;
    }

    public String getDepName() {
        return DepName;
    }

    public void setDepName(String depName) {
        DepName = depName;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(Name);
        out.writeUTF(Sex);
        out.writeInt(Age);
        out.writeInt(DepNo);
        out.writeUTF(DepName);
        out.writeUTF(table);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.Name = in.readUTF();
        this.Sex = in.readUTF();
        this.Age = in.readInt();
        this.DepNo = in.readInt();
        this.DepName = in.readUTF();
        this.table = in.readUTF();
    }

    //不做任何排序
    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public String toString() {
        return "EmpJoinDep [Name=" + Name + ", Sex=" + Sex + ", Age=" + Age
                + ", DepName=" + DepName + "]";
    }

}
