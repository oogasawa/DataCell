/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.ogalab.Pipe.out;

import java.io.IOException;
import java.util.ArrayList;
import net.ogalab.Pipe.Out;
import net.ogalab.datacell.container.DCContainer;
import net.ogalab.datacell.container.DCContainerFactory;
import net.ogalab.util.fundamental.StringUtil;
import org.apache.commons.configuration.ConfigurationException;

/**
 *
 * @author oogasawa
 */
public class DCOutByPuttingRow implements Out {

    private DCContainer dbObj = null;
    private String ds = null;
    private String pred = null;

    public DCOutByPuttingRow(DCContainerFactory facObj, String dbName, String ds, String pred) throws IOException {
        try {
            dbObj = facObj.getInstance(dbName);
        } catch (ConfigurationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
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
