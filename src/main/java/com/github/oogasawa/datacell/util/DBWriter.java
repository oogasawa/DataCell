package com.github.oogasawa.datacell.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

import com.github.oogasawa.datacell.DataCell;
import com.github.oogasawa.datacell.container.DCContainer;
import com.github.oogasawa.datacell.container.DCContainerFactory;

/**
 *
 * <h3>Example</h3>
 * <pre>{@code
 * cat file | DBWriter dbname "tdf4"
 * cat file | DBWriter dbname "json4"
 * cat file | DBWriter dbname data_set pred "tdf2"
 * cat file | DBWriter dbname data_set pred "json2"
 *
 * DBWriter dbname "tdf4" file
 * DBWriter dbname "json4" file
 * DBWriter dbname data_set pred "tdf2" file
 * DBWriter dbname data_set pred "json2" file
 * }</pre>
 *
 * <h3>Example of main() methods</h3>
 * <pre>{@code
 *     public static void main(String[] args) {
 *
 * DBWriter obj = new DBWriter();
 * String[] args2 = obj.setParameters(args, "DBWriter");
 *
 * String datafile = null;
 * String database = null;
 * try {
 * if (args2.length == 1) {
 * database = args2[0];
 * obj.writeToDB(System.in, database);
 * }
 * else if (args.length > 2) {
 * database = args2[0];
 * datafile = args2[1];
 * obj.writeToDB(new FileInputStream(datafile), database);
 * }
 * }
 * catch (Exception e) {
 * RuntimeExceptionUtil.invoke(e);
 * }
 * }
 *
 * }</pre>
 *
 * @author oogasawa
 *
 */
public class DBWriter {

    /*
    protected Parameter param = new Parameter();
    protected DCContainerFactory facObj = null;

    public DBWriter(DCContainerFactory facObj) {
        this.facObj = facObj;
    }

    public void cui(String[] args) {

        String[] args2 = setParameters(args, "DBWriter");

        String datafile = null;
        String database = null;
        try {
            //if (args2.length == 1) {
            //    database = args2[0];
            //    writeToDB(System.in, database);
            //} else if (args.length > 2) {
            if (args.length > 2) {
                database = args2[0];
                datafile = args2[1];
                writeToDB(datafile, database);
            }
        } catch (Exception e) {
            RuntimeExceptionUtil.invoke(e);
        }
    }

    public Parameter getParameter() {
        return param;
    }

    public void setParameter(Parameter param) {
        this.param = param;
    }

    private String[] setParameters(String[] args, String progName) {

        param.setDefault("DBName", "default_db");
        param.setDefault("Format", "tdf4");
        param.setDefault("DataSet", "aaa");
        param.setDefault("Predicate", "bbb");

        String[] otherArgs = null;
        // Set parameters on the object.
        param.setDefault("ProgramName", progName);
        otherArgs = param.parseCommandLine(args, progName);
        param.printValidParameters();

        return otherArgs;
    }

    public void writeToDB(String datafile, String database) throws FileNotFoundException {

        //if (!param.getString("DeleteTableIfExists").equalsIgnoreCase("false")) {
        //    deleteTableIfExists(database);
        //}
        if (param.getString("Format").toLowerCase().equals("tdf4")) {
            InputStream istr = new FileInputStream(datafile);
            tdf4ToDB(istr, database);
        } else if (param.getString("Format").toLowerCase().equals("tdf2")) {
            InputStream istr = new FileInputStream(datafile);
            tdf2ToDB(istr, database);
        } else if (param.getString("Format").toLowerCase().equals("json4")) {
            InputStream istr = new FileInputStream(datafile);
            json4ToDB(istr, database);
        } else if (param.getString("Format").toLowerCase().equals("json2")) {
            InputStream istr = new FileInputStream(datafile);
            json2ToDB(istr, database);
        } else if (param.getString("Format").toLowerCase().equals("dctext")) {
            dctextToDB(datafile, database);
        }

    }

    public void deleteTableIfExists(String database) {
        ArrayList<String> arg = StringUtil.splitByComma(param.getString("DeleteTableIfExists"));
        String dataSet = arg.get(0);
        String predicate = arg.get(1);
        DCContainer dbObj = null;
        try {
            dbObj = facObj.getInstance(database);
            dbObj.deleteTableIfExists(dataSet, predicate);
        } catch (Exception e) {
            RuntimeExceptionUtil.invoke(e);
        } finally {
            if (dbObj != null) {
                dbObj.close();
            }
        }
    }

    public void json4ToDB(InputStream istr, String database) {
        DCContainer dbObj = null;
        BufferedReader br = null;
        DataCell cell = new DataCell();
        try {
            dbObj = facObj.getInstance(database);
            br = new BufferedReader(new InputStreamReader(istr));

            while (cell.iterateJSON4(br) != null) {
                dbObj.putRow(cell.getDataSet(), cell.getID(), cell.getPredicate(), cell.getValue());
            }
        } catch (Exception e) {
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

    public void json2ToDB(InputStream istr, String database) {
        DCContainer dbObj = null;
        BufferedReader br = null;
        DataCell cell = new DataCell();
        cell.setDataSet(param.getString("DataSet"));
        cell.setPredicate(param.getString("Predicate"));
        try {
            dbObj = facObj.getInstance(database);
            br = new BufferedReader(new InputStreamReader(istr));
            while (cell.iterateJSON2(br) != null) {
                dbObj.putRow(cell.getDataSet(), cell.getID(), cell.getPredicate(), cell.getValue());
            }
        } catch (Exception e) {
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

    public void tdf4ToDB(InputStream istr, String database) {
        DCContainer dbObj = null;
        BufferedReader br = null;
        DataCell cell = new DataCell();
        try {
            dbObj = facObj.getInstance(database);
            br = new BufferedReader(new InputStreamReader(istr));
            String line = null;
            while ((line = br.readLine()) != null) {
                cell.readTSV4(line);
                dbObj.putRow(cell.getDataSet(), cell.getID(), cell.getPredicate(), cell.decodeTSV(cell.getValue()));
            }
        } catch (Exception e) {
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

    public void tdf2ToDB(InputStream istr, String database) {
        DCContainer dbObj = null;
        BufferedReader br = null;
        DataCell cell = new DataCell();
        cell.setDataSet(param.getString("DataSet"));
        cell.setPredicate(param.getString("Predicate"));
        try {
            dbObj = facObj.getInstance(database);
            br = new BufferedReader(new InputStreamReader(istr));
            String line = null;
            while ((line = br.readLine()) != null) {
                cell.readTSV2(line);
                dbObj.putRow(cell.getDataSet(), cell.getID(), cell.getPredicate(), cell.decodeTSV(cell.getValue()));
            }
        } catch (Exception e) {
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

    public void dctextToDB(String datafile, String dbName) {
        DCTextParser p = new DCTextParser();
        p.parse(new File(datafile), facObj, dbName);
    }
*/

}
