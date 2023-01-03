package com.github.oogasawa.datacell.mem;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.github.oogasawa.datacell.DataCell;
import com.github.oogasawa.datacell.container.DCContainer;
import com.github.oogasawa.utility.files.FileIO;

/**
 *
 * @author oogasawa
 */
public class MemDbUtil {
    
    private static final Logger logger = Logger.getLogger("com.github.oogasawa.datacell.mem");
    
    /**
     * Dumps all tables in a given directory outdir.
     *
     * This method also write out all the management tables,
     *
     * ORIGINAL_NAME__INTERNAL_NAME, INTERNAL_NAME__ORIGINAL_NAME,
     * INTERNAL_NAME_PREFIX__MAX_COUNT
     *
     *
     * @param memObj A MemDB object.
     * @param outdir A directory in which MemDB tables are write out.
     *
     */
    public static void dumpMemDB(DCContainer memObj, String outdir) {

        File dirObj = new File(outdir);
        if (!dirObj.exists()) {
            dirObj.mkdirs();
        }

        //StringBuilder sb = new StringBuilder();
        ArrayList<String> tableNames = memObj.getListOfAllTables();
        for (String tableName : tableNames) {
            saveTableAsTsv(memObj, outdir, tableName);
        }

    }

    
    public static void saveTable(DCContainer memObj, String ds, String pred, String tsv2Path) {
        
        logger.fine("ds, pred :" + ds + "\t" + pred);
        
        PrintWriter pw = null;
        try {
            pw = FileIO.getPrintWriter(tsv2Path);
            for (DataCell cell : memObj.setIterableTable(ds, pred)) {
                pw.write(cell.asTSV2() + "\n");
            }
        } catch (IOException ex) {
            Logger.getLogger(MemDB.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (pw != null) {
                pw.close();
            }
        }
    }
    
    
    
    public static void saveTableAsTsv(DCContainer memObj, String dirName, String tableName) {
        PrintWriter pw = null;
        try {
            pw = FileIO.getPrintWriter(dirName + "/" + tableName + ".tsv");
            for (DataCell cell : memObj.setIterableTable(tableName)) {
                pw.write(cell.asTSV2() + "\n");
            }
        } catch (IOException ex) {
            Logger.getLogger(MemDB.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (pw != null) {
                pw.close();
            }
        }

    }

    public static void loggingDsPredList(DCContainer memObj) {

        ArrayList<String> dsList = memObj.getDataSetList();
        for (String ds : dsList) {
            ArrayList<String> predList = memObj.getPredicateList(ds);
            for (String pred : predList) {
                
                int counter = 0;
                for (DataCell cell : memObj.setIterableTable(ds, pred))
                    counter++;
                
                logger.fine("ds , pred, number of rows : " + ds + "\t" + pred + "\t" + counter);
            }
        }
        
        

    }
    
 
}
