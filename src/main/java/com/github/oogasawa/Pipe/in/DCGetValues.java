package com.github.oogasawa.Pipe.in;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Logger;

import com.github.oogasawa.Pipe.In;
import com.github.oogasawa.Pipe.Pipe;
import com.github.oogasawa.datacell.DataCell;
import com.github.oogasawa.datacell.container.DCContainer;
import com.github.oogasawa.datacell.container.DCContainerFactory;

import org.apache.commons.configuration2.ex.ConfigurationException;

public class DCGetValues implements In {

    private static final Logger logger = Logger.getLogger("com.github.oogasawa.Pipe");

    private String dbName;
    private String dataset;
    private String objectId;
    private String predicate;
    
    private DCContainer dbObj = null;
    private ArrayList<String> valueList = null;
    private Iterator<String> iter = null;

    public DCGetValues(String dbName, String dataset, String objectId, String predicate, DCContainerFactory facObj) {
        try {

            this.dbName = dbName;
            this.dataset = dataset;
            this.objectId = objectId;
            this.predicate = predicate;
            
            dbObj = facObj.getInstance(dbName);
            valueList = dbObj.getValueList(dataset, objectId, predicate);

        } catch (ConfigurationException e) {
            logger.throwing("com.github.oogasawa.Pipe.in.DCCat", "constructor", e);
            logger.warning("Runtime exception in a DCCat constructor. ");
        }

    }

    public String getLine() {
        String result = Pipe.END;
        DataCell row = null;

        try {
            if (iter == null) {
                iter = valueList.iterator();
            }

            if (iter.hasNext()) {
                row = new DataCell(this.dataset, this.objectId, this.predicate, iter.next());
                result = row.asTSV2();
            }
            else {
                result = Pipe.END;
            }
        } catch (Exception e) {
            logger.throwing("com.github.oogasawa.Pipe.in.DCGetValues", "getLine", e);
        }

        return result;
    }

    public void close() {
        if (dbObj != null) {
            dbObj.close();
        }
    }

}
