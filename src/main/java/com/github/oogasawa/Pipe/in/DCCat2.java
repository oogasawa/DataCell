package com.github.oogasawa.Pipe.in;

import java.util.Iterator;
import java.util.logging.Logger;

import com.github.oogasawa.Pipe.In;
import com.github.oogasawa.Pipe.Pipe;
import com.github.oogasawa.datacell.DataCell;
import com.github.oogasawa.datacell.container.DCContainer;


/**
 *
 * @author oogasawa
 */
public class DCCat2 implements In {

    private static final Logger logger = Logger.getLogger("com.github.oogasawa.Pipe");
    
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
            logger.throwing("com.github.oogasawa.Pipe.in.DCCat2", "getLine", e);
        }

        return result;
    }

    public void close() {
        if (memObj != null) {
            memObj.close();
        }
    }

}
