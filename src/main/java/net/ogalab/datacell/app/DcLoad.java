/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.ogalab.datacell.app;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.ogalab.datacell.DataCell;
import net.ogalab.datacell.container.DCContainer;
import net.ogalab.datacell.container.DCContainerFactory;
import net.ogalab.microutil.exception.RuntimeExceptionUtil;
import net.ogalab.microutil.type.StringUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 *
 * @author oogasawa
 */
abstract public class DcLoad {
    
    protected DCContainerFactory facObj = null;

    
    public void run(String[] args) {

        CommandLine cmd = analyzeArgs(args);

        initDCContainerFactory();
        
        String dbName  = cmd.getOptionValue("db");
        String dataSet = cmd.getOptionValue("ds");
        String pred    = cmd.getOptionValue("p");
        String op      = cmd.getOptionValue("op");
        if (op == null)
            op = "s";
        String format      = cmd.getOptionValue("f");
        if (format == null)
            format = "TSV2";
        if (format.equals("TSV2") && (dataSet == null || pred == null)) {
            printHelp();
        }
        
        
        
        List<String> otherArgs = cmd.getArgList();
        InputStream in = null;
        if (otherArgs.size() == 0) {
            in = System.in;
        }
        else {
            try {
                in = new FileInputStream(otherArgs.get(0));
            } catch (FileNotFoundException ex) {
                Logger.getLogger(DcLoad.class.getName()).log(Level.SEVERE, null, ex);
            }            
        }
 
                
        if (format.equals("TSV2"))         
            loadTsv2(dbName, dataSet, pred, in, op);
        else if (format.equals("TSV4"))
            loadTsv4(dbName, in, op);
        
    }

    abstract public void initDCContainerFactory();
    
    
    
    public void loadTsv2(String dbName, String dataSet, String pred, InputStream in, String op) {
        
        DCContainer dbObj = null;
        BufferedReader br = null;
        
        DataCell cell = new DataCell();
        cell.setDataSet(dataSet);
        cell.setPredicate(pred);
        
        try {
            dbObj = facObj.getInstance(dbName);
            br = new BufferedReader(new InputStreamReader(in));
            String line = null;
            
            if (op.equals("s")) {
            
                while ((line = br.readLine()) != null) {
                    cell.readTSV2(line);
                    dbObj.putRow(cell.getDataSet(), cell.getID(), cell.getPredicate(), cell.decodeTSV(cell.getValue()));
                }
            }
            else if (op.equals("kv")) {
            
                while ((line = br.readLine()) != null) {
                    cell.readTSV2(line);
                    dbObj.putRowIfKeyValuePairIsAbsent(cell.getDataSet(), cell.getID(), cell.getPredicate(), cell.decodeTSV(cell.getValue()));
                }
            }
            else if (op.equals("k")) {
            
                while ((line = br.readLine()) != null) {
                    cell.readTSV2(line);
                    dbObj.putRowIfKeyIsAbsent(cell.getDataSet(), cell.getID(), cell.getPredicate(), cell.decodeTSV(cell.getValue()));
                }
            }           
            else if (op.equals("r")) {
            
                while ((line = br.readLine()) != null) {
                    cell.readTSV2(line);
                    dbObj.putRowWithReplacingValues(cell.getDataSet(), cell.getID(), cell.getPredicate(), cell.decodeTSV(cell.getValue()));
                }
            }
            
            
            
        } catch (Exception e) {
            e.printStackTrace();
            RuntimeExceptionUtil.invoke(e);
        } finally {
            if (dbObj != null) {
                dbObj.close();
            }

            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }


    public void loadTsv4(String dbName, InputStream in, String op) {
        
        DCContainer dbObj = null;
        BufferedReader br = null;
        
        DataCell cell = new DataCell();
        
        try {
            dbObj = facObj.getInstance(dbName);
            br = new BufferedReader(new InputStreamReader(in));
            String line = null;
            
            if (op.equals("s")) {
            
                while ((line = br.readLine()) != null) {
                    cell.readTSV4(line);
                    dbObj.putRow(cell.getDataSet(), cell.getID(), cell.getPredicate(), cell.decodeTSV(cell.getValue()));
                }
            }
            else if (op.equals("kv")) {
            
                while ((line = br.readLine()) != null) {
                    cell.readTSV4(line);
                    dbObj.putRowIfKeyValuePairIsAbsent(cell.getDataSet(), cell.getID(), cell.getPredicate(), cell.decodeTSV(cell.getValue()));
                }
            }
            else if (op.equals("k")) {
            
                while ((line = br.readLine()) != null) {
                    cell.readTSV4(line);
                    dbObj.putRowIfKeyIsAbsent(cell.getDataSet(), cell.getID(), cell.getPredicate(), cell.decodeTSV(cell.getValue()));
                }
            }           
            else if (op.equals("r")) {
            
                while ((line = br.readLine()) != null) {
                    cell.readTSV4(line);
                    dbObj.putRowWithReplacingValues(cell.getDataSet(), cell.getID(), cell.getPredicate(), cell.decodeTSV(cell.getValue()));
                }
            }
            
            
            
        } catch (Exception e) {
            e.printStackTrace();
            RuntimeExceptionUtil.invoke(e);
        } finally {
            if (dbObj != null) {
                dbObj.close();
            }

            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    private void printHelp() {
        System.err.println("USAGE: ");
        System.err.println("dc_load -db db_name -f TSV2 -ds dataset -p pred -op s");
        System.err.println("dc_load -db db_name -f TSV4 -op s");
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
                    .desc("Database name")
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
                    .argName("pred")
                    .desc("Name of a predicate.")
                    .build());
            
            
            options.addOption(Option.builder("op")
                    .longOpt("operation")
                    .required(false)
                    .hasArg()
                    .argName("op")
                    .desc("Type of loading operation. (s, kv, k, r)")
                    .build());

            options.addOption(Option.builder("f")
                    .longOpt("format")
                    .required(false)
                    .hasArg()
                    .argName("format")
                    .desc("Format of input file. (TSV2, TSV4)")
                    .build());

            
            CommandLineParser parser = new DefaultParser();
            cmd = parser.parse(options, args);

        } catch (ParseException ex) {
            Logger.getLogger(DcList.class
                    .getName()).log(Level.SEVERE, null, ex);
        }

        return cmd;
    }    
}
