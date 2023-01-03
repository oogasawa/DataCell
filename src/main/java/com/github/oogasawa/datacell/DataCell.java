package com.github.oogasawa.datacell;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.github.oogasawa.utility.types.collection.ListUtil;
import com.github.oogasawa.utility.types.string.StringUtil;

import net.arnx.jsonic.JSON;


public class DataCell {

    public static final int TSV4 = 1;
    public static final int TSV2 = 2;
    public static final int JSON4 = 3;
    public static final int JSON2 = 4;

    String dataSet = "";
    String ID = "";
    String predicate = "";
    String value = "";

    int format = 2;

    Pattern pJsonDelim = Pattern.compile("^#---");

    public DataCell() {
    }

    public DataCell(String ds, String id, String pred, String value) {
        dataSet = ds;
        ID = id;
        predicate = pred;
        this.value = value;
    }

    public String encodeTSV(String str) {
        return StringUtil.asOneLine(str);
    }

    public String decodeTSV(String str) {
        return StringUtil.asMultiLines(str);
    }

    public boolean isJSON() {
        if (format == JSON2 || format == JSON4) {
            return true;
        } else {
            return false;
        }
    }

    public int getFormat() {
        return this.format;
    }

    public void setFormat(int f) {
        this.format = f;
    }

    public void setFormat(String f) {
        if (f.toLowerCase().equals("tsv4")) {
            this.format = TSV4;
        } else if (f.toLowerCase().equals("tsv2")) {
            this.format = TSV2;
        } else if (f.toLowerCase().equals("json4")) {
            this.format = JSON4;
        } else if (f.toLowerCase().equals("json2")) {
            this.format = JSON2;
        }
    }

    public String toString() {
        String result = null;
        if (this.format == TSV4) {
            result = asTSV4();
        } else if (this.format == TSV2) {
            result = asTSV2();
        } else if (this.format == JSON4) {
            result = asJSON4();
        } else if (this.format == JSON2) {
            result = asJSON2();
        }

        return result;
    }

    public String asTSV4() {
        ArrayList<String> result = new ArrayList<String>();
        result.add(dataSet);
        result.add(ID);
        result.add(predicate);
        result.add(StringUtil.asOneLine(value));

        return ListUtil.join("\t", result);
    }

    public String asTSV2() {
        ArrayList<String> result = new ArrayList<String>();
        result.add(ID);
        result.add(StringUtil.asOneLine(value));

        return ListUtil.join("\t", result);
    }

    public void readTSV2(String tsv2) {
        ArrayList<String> cols = StringUtil.splitByTab(tsv2);
        ID = cols.get(0);
        value = decodeTSV(cols.get(1));
    }

    public void readTSV4(String tsv4) {
        ArrayList<String> cols = StringUtil.splitByTab(tsv4);
        dataSet = cols.get(0);
        ID = cols.get(1);
        predicate = cols.get(2);
        value = decodeTSV(cols.get(3));
    }

    public String asJSON4() {
        DataCell4 c4 = new DataCell4();
        c4.setData_set(dataSet);
        c4.setID(ID);
        c4.setPredicate(predicate);
        c4.setValue(value);

        String result = JSON.encode(c4, true);
        return result;
    }

    public String asJSON2() {
        DataCell2 c2 = new DataCell2();
        c2.setID(ID);
        c2.setValue(value);

        String result = JSON.encode(c2, true);
        return result;
    }

    public String getNextJsonString(BufferedReader br) throws IOException {
        StringBuilder sb = new StringBuilder();
        String result = null;
        String line = null;
        while (true) {
            line = br.readLine();

            if (line == null) {
                if (sb.length() > 0) {
                    result = sb.toString();
                    sb.delete(0, sb.length() - 1);
                    break;
                } else {
                    result = null;
                    break;
                }
            } else {
                Matcher m = pJsonDelim.matcher(line);
                if (m.find()) {
                    if (sb.length() > 0) {
                        result = sb.toString();
                        sb.delete(0, sb.length() - 1);
                        break;
                    } else {
                        continue;
                    }
                } else {
                    sb.append(line + "\n");
                }
            }

        }
        return result;
    }

    public void readJSON4(String json) throws IOException {
        DataCell4 c4 = JSON.decode(json, DataCell4.class);
        this.dataSet = c4.getData_set();
        this.ID = c4.getID();
        this.predicate = c4.getPredicate();
        this.value = c4.getValue();
    }

    public void readJSON2(String json) throws IOException {
        DataCell2 c2 = JSON.decode(json, DataCell2.class);
        this.ID = c2.getID();
        this.value = c2.getValue();
    }

    public String iterateJSON4(BufferedReader br) throws IOException {
        String json = getNextJsonString(br);
        if (json == null) {
            this.clear();
        } else {
            readJSON4(json);
        }

        return json;
    }

    public String iterateJSON2(BufferedReader br) throws IOException {
        String json = getNextJsonString(br);
        if (json == null) {
            this.clear();
        } else {
            readJSON2(json);
        }

        return json;
    }

    public void clear() {
        dataSet = "";
        ID = "";
        predicate = "";
        value = "";
    }

    public void setData(String ds, String id, String pred, String val) {
        dataSet = ds;
        ID = id;
        predicate = pred;
        value = val;
    }

    public String getDataSet() {
        return dataSet;
    }

    public void setDataSet(String dataSet) {
        this.dataSet = dataSet;
    }

    public String getID() {
        return ID;
    }

    public void setID(String id) {
        this.ID = id;
    }

    public String getPredicate() {
        return predicate;
    }

    public void setPredicate(String predicate) {
        this.predicate = predicate;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
