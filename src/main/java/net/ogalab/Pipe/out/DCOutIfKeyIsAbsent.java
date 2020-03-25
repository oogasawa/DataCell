package net.ogalab.Pipe.out;

import java.io.IOException;
import java.util.ArrayList;

import net.ogalab.Pipe.Out;
import net.ogalab.datacell.container.DCContainer;
import net.ogalab.datacell.container.DCContainerFactory;
import net.ogalab.microutil.type.StringUtil;

import org.apache.commons.configuration.ConfigurationException;

public class DCOutIfKeyIsAbsent implements Out {

    private DCContainer dbObj = null;
    private String ds = null;
    private String pred = null;

    public DCOutIfKeyIsAbsent(DCContainerFactory facObj, String dbName, String ds, String pred) throws IOException {
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
        dbObj.putRowIfKeyIsAbsent(ds, col.get(0), pred, col.get(1));
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