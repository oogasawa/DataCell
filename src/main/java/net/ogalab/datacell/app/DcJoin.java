/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.ogalab.datacell.app;

import java.io.FileNotFoundException;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.ogalab.Pipe.PipeLine;
import net.ogalab.Pipe.filter.DCJoin;
import net.ogalab.Pipe.in.StdIn;
import net.ogalab.Pipe.out.StdOut;
import net.ogalab.datacell.container.DCContainerFactory;
import net.ogalab.microutil.type.Type;
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
abstract public class DcJoin {

    protected DCContainerFactory facObj = null;

    public void run(String[] args) {

        CommandLine cmd = analyzeArgs(args);
        
        initDCContainerFactory();

        String dbName = cmd.getOptionValue("db");
        String dataSet = cmd.getOptionValue("ds");
        String pred = cmd.getOptionValue("p");

        int t = 0;
        if (cmd.hasOption("t")) {
            t = Type.to_int(cmd.getOptionValue("t"));
        }

        boolean c = false;
        if (cmd.hasOption("c")) {
            c = true;
        }

        join(dbName, dataSet, pred, t, c);

    }

    abstract public void initDCContainerFactory();

    public void join(String dbName, String dataSet, String pred, int t, boolean c) {

        PipeLine pl = null;
        try {

            pl = new PipeLine(new StdIn(), new StdOut());
            
            if (c) {
                pl.add(new DCJoin(dbName, dataSet, pred, facObj).setTargetColumn(t).setCollapsed(true));
            } else {
                pl.add(new DCJoin(dbName, dataSet, pred, facObj).setTargetColumn(t));
            }
            
            pl.start();

        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(DcJoin.class.getName()).log(Level.SEVERE, null, ex);
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
                    .required()
                    .hasArg()
                    .argName("dataSet")
                    .desc("Name of a data set.")
                    .build());

            options.addOption(Option.builder("p")
                    .longOpt("pred")
                    .required()
                    .hasArg()
                    .argName("predicate")
                    .desc("Name of a predicate.")
                    .build());

            options.addOption(Option.builder("t")
                    .longOpt("target")
                    .required(false)
                    .hasArg()
                    .argName("target")
                    .desc("Target column.")
                    .build());

            options.addOption(Option.builder("c")
                    .longOpt("collapse")
                    .required(false)
                    .argName("target")
                    .desc("Collapse multiple values into one, or not")
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
