package com.github.oogasawa.Pipe.out;

import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Logger;

import com.github.oogasawa.Pipe.Out;
import com.github.oogasawa.datacell.container.DCContainer;
import com.github.oogasawa.datacell.container.DCContainerFactory;
import com.github.oogasawa.utility.types.string.StringUtil;

import org.apache.commons.configuration2.ex.ConfigurationException;



/**
 *
 * @author oogasawa
 */
public class DCOutByPuttingRow implements Out {

    private static final Logger logger = Logger.getLogger("com.github.oogasawa.Pipe");
    
    private DCContainer dbObj = null;
    private String ds = null;
    private String pred = null;

    public DCOutByPuttingRow(DCContainerFactory facObj, String dbName, String ds, String pred) throws IOException {
        try {
            dbObj = facObj.getInstance(dbName);
        } catch (ConfigurationException e) {
            logger.throwing("com.github.oogasawa.Pipe.out.DCOutByPuttingRow", "constructor", e);
            logger.warning("Runtime exception in a DCOutByPuttingRow constructor. ");
        }
        this.ds = ds;
        this.pred = pred;
    }

    public void putLine(String line) {
        ArrayList<String> col = StringUtil.splitByTab(line);
        dbObj.putRow(ds, col.get(0), pred, col.get(1));
    }

    public void end() {
        if (dbObj != null) {
            dbObj.close();
        }
    }

    public Object get() {
        return null;
    }

}
