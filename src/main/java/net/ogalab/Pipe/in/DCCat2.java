/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.ogalab.Pipe.in;

import java.util.Iterator;
import net.ogalab.Pipe.In;
import net.ogalab.Pipe.Pipe;
import net.ogalab.datacell.DataCell;
import net.ogalab.datacell.container.DCContainer;
import net.ogalab.microutil.exception.RuntimeExceptionUtil;

/**
 *
 * @author oogasawa
 */
public class DCCat2 implements In {

    //private BufferedReader reader;

    private DCContainer memObj = null;
    public Iterator<DataCell> iter = null;

    public DCCat2(String dataset, String predicate, DCContainer memObj) {
        this.memObj = memObj;
        this.memObj.setIterableTable(dataset, predicate);

    }

    public String getLine() {
        String result = Pipe.END;
        DataCell row = null;
        //Iterator<ArrayList<String>> iter = dbObj.iterator();
        try {
            if (iter == null) {
                iter = memObj.iterator();
            }

            if (iter.hasNext()) {
                row = iter.next();
                result = row.asTSV2();
            }
        } catch (Exception e) {
            RuntimeExceptionUtil.invoke(e, "Runtime error in CDBCat.getLine() ");
        }

        return result;
    }

    public void close() {
        if (memObj != null) {
            memObj.close();
        }
    }

}
