/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.ogalab.datacell.app;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.ogalab.datacell.DataCell;
import net.ogalab.datacell.container.DCContainer;
import net.ogalab.datacell.container.DCContainerFactory;
import net.ogalab.microutil.exception.RuntimeExceptionUtil;
import org.apache.commons.cli.*;
import org.apache.commons.configuration.ConfigurationException;

/**
 *
 * @author oogasawa
 */
abstract public class DcList {

    protected HashSet<String> blackList = new HashSet<String>();
    protected DCContainerFactory facObj = null;    

    public void run(String[] args) {

        CommandLine cmd = analyzeArgs(args);  
        
        initDCContainerFactory();

        if (cmd.hasOption("db") && cmd.hasOption("ds") && cmd.hasOption("p")) {
            String dbName  = cmd.getOptionValue("db");
            String dataSet = cmd.getOptionValue("ds");
            String pred    = cmd.getOptionValue("p");
            
            listRows(dbName, dataSet, pred);
        }
        else if (cmd.hasOption("db") && cmd.hasOption("ds")) {
            String dbName  = cmd.getOptionValue("db");
            String dataSet = cmd.getOptionValue("ds");
            
            listPredicates(dbName, dataSet);
        }
        else if (cmd.hasOption("db")) {
            String dbName  = cmd.getOptionValue("db");
            
            listDataSets(dbName);
        }
    }
    

    public DcList() {
        blackList.add("INTERNAL_NAME");
        blackList.add("ORIGINAL_NAME");
        blackList.add("INTERNAL_NAME_PREFIX");
    }

    
    abstract public void initDCContainerFactory();
    
    
    public void listRows(String dbName, String dataSet, String predicate) {

        DCContainer dbObj = null;
        try {
            dbObj = facObj.getInstance(dbName);

            for (DataCell cell : dbObj.setIterableTable(dataSet, predicate)) {
                System.out.println(cell);
            }
        } catch (ConfigurationException e) {
            RuntimeExceptionUtil.invoke(e, "ERROR: Can not open the database " + dbName);
        } finally {
            dbObj.close();
        }
    }
    
    

    public void listDataSets(String dbName) {

        DCContainer dbObj = null;
        try {
            int counter = 0;
            dbObj = facObj.getInstance(dbName);
            ArrayList<String> dsList = dbObj.getDataSetList();
            for (String ds : dsList) {
                if (ds == null) {
                    continue;
                }
                if (blackList.contains(ds)) {
                    continue;
                }
                ArrayList<String> predList = dbObj.getPredicateList(ds);
                counter++;
                System.out.println(counter + "\t" + ds + "\t" + predList.size());
            }

        } catch (ConfigurationException e) {
            RuntimeExceptionUtil.invoke(e, "ERROR: Can not open the database " + dbName);
        } finally {
            dbObj.close();
        }

    }

    public void listPredicates(String dbName, String dataSet) {

        DCContainer dbObj = null;
        try {
            dbObj = facObj.getInstance(dbName);

            int counter = 0;

            ArrayList<String> predList = dbObj.getPredicateList(dataSet);
            for (String pred : predList) {
                if (!blackList.contains(dataSet) && !blackList.contains(pred)) {
                    int numOfRows = getNumOfRows(dbObj, dataSet, pred);
                    System.out.println(++counter + "\t" + dataSet + "\t" + pred + "\t" + numOfRows);
                }
            }

        } catch (ConfigurationException e) {
            RuntimeExceptionUtil.invoke(e, "ERROR: Can not open the database " + dbName);
        } finally {
            dbObj.close();
        }

    }

    private int getNumOfRows(DCContainer dbObj, String dataSet, String pred) {
        int num = 0;
        for (@SuppressWarnings("unused") DataCell row : dbObj.setIterableTable(dataSet, pred)) {
            num++;
        }
        return num;
    }

    
    private CommandLine analyzeArgs(String[] args) {

        Options options = new Options();
        CommandLine cmd = null;
        
        try {
            options.addOption(Option.builder("db")
                    .longOpt("database")
                    .required()
                    .hasArg()
                    .argName("dbName")
                    .desc("Database Name")
                    .build());
            
              options.addOption(Option.builder("ds")
                    .longOpt("dataset")
                    .required(false)
                    .hasArg()
                    .argName("dataSet")
                    .desc("Name of a data set.")
                    .build());
          
              options.addOption(Option.builder("p")
                    .longOpt("pred")
                    .required(false)
                    .hasArg()
                    .argName("predicate")
                    .desc("Name of a predicate.")
                    .build());
             
              options.addOption(Option.builder("f")
                    .longOpt("format")
                    .required(false)
                    .hasArg()
                    .argName("format")
                    .desc("Output format: (tsv, json)")
                    .build());
              
              
            
            CommandLineParser parser = new DefaultParser();
            cmd = parser.parse(options, args);
            
 
        } catch (ParseException ex) {
            Logger.getLogger(DcList.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return cmd;
    }

}
