/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package net.ogalab.datacell.mem;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.ogalab.datacell.DataCell;
import net.ogalab.datacell.container.DCContainer;
import net.ogalab.datacell.mem.MemDB;
//import net.ogalab.dataupdator.ex.phase2a.SraSampleAndNcbiTaxonomySt2;
import net.ogalab.util.os.FileIO;
import org.slf4j.LoggerFactory;

/**
 *
 * @author oogasawa
 */
public class MemDbUtil {
    
     protected static org.slf4j.Logger logger = LoggerFactory.getLogger(MemDbUtil.class);
    
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
        
        logger.debug("ds, pred :" + ds + "\t" + pred);
        
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
                
                logger.debug("ds , pred, number of rows : " + ds + "\t" + pred + "\t" + counter);
            }
        }
        
        

    }
    
 
}
