/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package net.ogalab.Pipe.filter;

import java.util.ArrayList;
import net.ogalab.Pipe.In;
import net.ogalab.Pipe.Out;
import net.ogalab.Pipe.Pipe;
import net.ogalab.datacell.container.DCContainer;
import net.ogalab.datacell.container.DCContainerFactory;
import net.ogalab.util.container.ListUtil;
import net.ogalab.util.exception.RuntimeExceptionUtil;
import net.ogalab.util.fundamental.StringUtil;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.LoggerFactory;

/**
 *
 * @author oogasawa
 */
public class DCJoin2 extends Filter {
    
    protected static org.slf4j.Logger logger = LoggerFactory.getLogger(DCJoin2.class);

    public final static int COLLAPSED = 1;
    public final static int REVERSED = 2;

    int targetColumn = 0;
    String delimiter = ";";
    String notAvailable = "\\N";
    boolean collapsed = false;
    boolean removeNewLines = true;
    boolean removeTabs = true;
    //boolean reversed  = false;
    DCContainer dbObj = null;
    String tableName = null;
    String dataset = null;
    String predicate = null;

    public DCJoin2(In in, Out out, String dbName, String dataset, String predicate, DCContainer dbObj) {
        super(in, out);

        this.dataset = dataset;
        this.predicate = predicate;
        this.dbObj     = dbObj;
    }

    public DCJoin2(String dbName, String dataset, String predicate, DCContainer dbObj) {
        super(null, null);

        this.dataset = dataset;
        this.predicate = predicate;
        this.dbObj     = dbObj;
    }

    public void run() {

        try {

            if (collapsed) {
                CollapsedJoin();
            } else {
                leftOuterJoin();
            }

        } catch (Exception e) {
            RuntimeExceptionUtil.invoke(e, "Runtime error in a DCJoin2.run() ");
        }

    }

    public void CollapsedJoin() throws InterruptedException {
        String line = Pipe.END;
        ArrayList<String> columns = null;
        while ((line = in.getLine()) != Pipe.END) {
            columns = StringUtil.splitByTab(line);

            ArrayList<String> values0 = dbObj.getValueList(dataset, columns.get(targetColumn), predicate);
            ArrayList<String> values = ListUtil.sort(ListUtil.unique(values0));
            if (values != null && values.size() > 0) {
                String v = ListUtil.join(delimiter, values);

                if (isRemoveNewLines()) {
                    v = StringUtil.removeNewLines(v);
                }

                if (isRemoveTabs()) {
                    v = StringUtil.removeTabs(v);
                }

                v = v.trim();
                if (v == null || v.length() == 0) {
                    v = notAvailable;
                }
                out.putLine(line.trim() + "\t" + v);
            } else {
                out.putLine(line.trim() + "\t" + notAvailable);
            }
        }
        out.end();
    }
    
    public void leftOuterJoin() throws InterruptedException {
        String line = Pipe.END;
        ArrayList<String> columns = null;
        ArrayList<String> values  = null;
        
        //logger.debug("ds, pred, targetColumn : " + dataset + "\t" + predicate + "\t" + targetColumn);
        
        while ((line = in.getLine()) != Pipe.END) {
            columns = StringUtil.splitByTab(line);

            //logger.debug("line : " + line);
            
            values = dbObj.getValueList(dataset, columns.get(targetColumn), predicate);

            
            if (values != null && values.size() > 0) {
                for (String v : values) {

                    v = StringUtil.asOneLine(v);

                    if (isRemoveNewLines()) {
                        v = StringUtil.removeNewLines(v);
                    }

                    if (isRemoveTabs()) {
                        v = StringUtil.removeTabs(v);
                    }

                    v = v.trim();
                    if (v == null || v.length() == 0) {
                        v = notAvailable;
                    }

                    out.putLine(line.trim() + "\t" + v);
                }
            } else {
                out.putLine(line.trim() + "\t" + notAvailable);
            }
        }
        out.end();
    }

    /*	
     public void ReversedCollapsedJoin() {
     String line = Pipe.END;
     ArrayList<String> columns = null;
     while ((line = in.getLine()) != Pipe.END) {
     columns = StringUtil.splitByTab(line);
			
     ArrayList<String> values = dbObj.getIdList(subject, predicate, columns.get(targetColumn));
     if (values != null && values.size() > 0) {
     String v = ListUtil.join(delimiter, values);
     out.putLine(line.trim() + "\t" + v);
     }
     else {
     out.putLine(line.trim() + "\t" + notAvailable);
     }
     }
     out.end();		
     }
	
     public void ReversedLeftOuterJoin() {
     String line = null;
     ArrayList<String> columns = null;
     while ((line = in.getLine()) != Pipe.END) {
     columns = StringUtil.splitByTab(line);

     ArrayList<String> values = dbObj.getIdList(subject, predicate, columns.get(targetColumn));
     if (values != null && values.size()>0) {
     for (String v : values) {
     out.putLine(line.trim() + "\t" + v);
     }
     }
     else {
     out.putLine(line.trim() + "\t" + notAvailable);
     }
     }
     out.end();		
     }
     */
    public boolean isCollapsed() {
        return collapsed;
    }

    public DCJoin2 setCollapsed(boolean collapsed) {
        this.collapsed = collapsed;

        return this;
    }
     
    public DCJoin2 setCollapsed(boolean collapsed, String delimiter) {
    	this.collapsed = collapsed;
    	this.delimiter = delimiter;
    	return this;
    }

    /*
     public boolean isReversed() {
     return reversed;
     }

     public DbJoin setReversed(boolean reversed) {
     this.reversed = reversed;
		
     return this;
     }
     */
    public int getTargetColumn() {
        return targetColumn;
    }

    public DCJoin2 setTargetColumn(int targetColumn) {
        this.targetColumn = targetColumn;

        return this;
    }

    public boolean isRemoveNewLines() {
        return removeNewLines;
    }

    public DCJoin2 setRemoveNewLines(boolean removeNewLine) {
        this.removeNewLines = removeNewLine;

        return this;
    }

    public boolean isRemoveTabs() {
        return removeNewLines;
    }

    public DCJoin2 setRemoveTabs(boolean removeTabs) {
        this.removeTabs = removeTabs;

        return this;
    }
    
}
