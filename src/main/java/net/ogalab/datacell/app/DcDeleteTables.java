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
import net.ogalab.datacell.container.DCContainer;
import net.ogalab.datacell.container.DCContainerFactory;
import net.ogalab.microutil.exception.RuntimeExceptionUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;

/**
 *
 * @author oogasawa
 */
abstract public class DcDeleteTables {

    protected HashSet<String> blackList = new HashSet<String>();
    protected DCContainerFactory facObj = null;

    public void run(String[] args) {

        CommandLine cmd = analyzeArgs(args);

        initDCContainerFactory();

        if (cmd.hasOption("db") && cmd.hasOption("ds") && cmd.hasOption("p")) {
            String dbName = cmd.getOptionValue("db");
            String dataSet = cmd.getOptionValue("ds");
            String pred = cmd.getOptionValue("p");

            deleteTable(dbName, dataSet, pred);
        } else if (cmd.hasOption("db") && cmd.hasOption("ds")) {
            String dbName = cmd.getOptionValue("db");
            String dataSet = cmd.getOptionValue("ds");

            deleteDataSet(dbName, dataSet);
        }

    }

    public DcDeleteTables() {
        blackList.add("INTERNAL_NAME");
        blackList.add("ORIGINAL_NAME");
        blackList.add("INTERNAL_NAME_PREFIX");
    }

    abstract public void initDCContainerFactory();

    public void deleteTable(String dbName, String dataSet, String predicate) {

        DCContainer dbObj = null;
        try {
            dbObj = facObj.getInstance(dbName);
            dbObj.deleteTable(dataSet, predicate);
        } catch (ConfigurationException e) {
            RuntimeExceptionUtil.invoke(e, "ERROR: Can not open the database " + dbName);
        } finally {
            dbObj.close();
        }
    }

    public void deleteDataSet(String dbName, String dataSet) {

        DCContainer dbObj = null;
        try {
            int counter = 0;
            dbObj = facObj.getInstance(dbName);
            ArrayList<String> predList = dbObj.getPredicateList(dataSet);
            for (String pred : predList) {
                if (pred == null) {
                    continue;
                } else if (blackList.contains(dataSet)) {
                    continue;
                } else {
                    dbObj.deleteTable(dataSet, pred);
                }
            }
        } catch (ConfigurationException e) {
            RuntimeExceptionUtil.invoke(e, "ERROR: Can not open the database " + dbName);
        } finally {
            dbObj.close();
        }

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

            CommandLineParser parser = new DefaultParser();
            cmd = parser.parse(options, args);

        } catch (ParseException ex) {
            Logger.getLogger(DcList.class.getName()).log(Level.SEVERE, null, ex);
        }

        return cmd;
    }

}
