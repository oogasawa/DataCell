package com.github.oogasawa.datacell.h2;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import com.github.oogasawa.datacell.container.DCContainer;
import com.github.oogasawa.pojobdd.BddUtil;
import com.github.oogasawa.utility.types.Type;

import org.apache.commons.configuration2.ex.ConfigurationException;

public class DataStoreSpec {

    public static boolean exec() {

        try (PrintStream out = BddUtil.newPrintStream("DataStoreSpec/DataStoreSpec.md")) {

            // Checks if all the tests are succeeded.
            List<Boolean> results = new ArrayList<Boolean>();
            results.add(storyDesc(out));
            results.add(basicExample01(out));

            out.flush();
            return BddUtil.allTrue(results);

        } catch (IOException ex) {
            ex.printStackTrace();
        } 

        return false;
    }


    public static boolean storyDesc(PrintStream out) {

        // Description
        String description = """
            ----
            id: DataStoreSpec
            title: DataStoring
            ---

            ## Description
            
            The database receives a sequence of the four value pairs and saves it.

            Here, a sequence of four pairs of values is as in the following example.
                        
            ```
            ncbi_taxonomy   1       parent_tax_id   1
            ncbi_taxonomy   1       rank    no rank
            ncbi_taxonomy   1       embl_code       \\N
            ncbi_taxonomy   1       division_id     8
            ncbi_taxonomy   1       inherited_div_flag      0
            ncbi_taxonomy   1       genetic_code_id 1
            ```

            
            The Database Management System (DBMS) used at this time
            can be changed according to the size of the data, the required speed, and the degree of parallelism.

            
            """;

        out.println(description);

        return true;
    }



    public static boolean basicExample01(PrintStream out) {

        String description = """

            ### Example 01 : basic use case

            
            Code:

            ```
            {{snippet}}
            ```

            Result:

            
            """;


        // Reality
        // %begin snippet : basicExample01
        
        StringBuilder result = new StringBuilder();
        
        H2Factory facObj = new H2Factory();
        String dbName = "./datacell_h2_test"; // "This must be started with ./".

        facObj.deleteDBIfExists(dbName);
        facObj.createDB(dbName);

        // In case of H2 database, the database is created when a new row is inserted.
        // Therefore, facObj.hasDB() will not be true 
        // immediately after invoking the createDB() method.
        result.append(String.format("facObj.hasDB(dbName) = %s\n", facObj.hasDB(dbName)));


        
        DCContainer dbObj = null;
        try {
            dbObj = facObj.getInstance(dbName);
            dbObj.putRow("ncbi_taxonomy", "1", "parent_tax_id", "1");
            String val = dbObj.getValue("ncbi_taxonomy", "1", "parent_tax_id");
            result.append(String.format("facObj.hasDB(dbName) = %b\n", facObj.hasDB(dbName)));
            result.append(String.format("value = %s\n", val));

        } catch (ConfigurationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            if (dbObj != null) {
                dbObj.close();
            }
        }

        facObj.deleteDB(dbName);
        result.append(String.format("facObj.hasDB(dbName) = %b\n", facObj.hasDB(dbName)));
        // %end snippet : basicExample01


        String snippet = BddUtil.readSnippet("src/test/java/com/github/oogasawa/datacell/h2/DataStoreSpec.java", "basicExample01");
        description = description.replace("{{snippet}}", snippet);
        out.println(description);

        // Expectations
        String expectation = """
            facObj.hasDB(dbName) = 1
            facObj.hasDB(dbName) = 0
            facObj.hasDB(dbName) = 1
            """;

        // Check the answer.
        boolean passed = BddUtil.assertTrue(out, expectation, result.toString());
        assert passed;
        return passed;

    }
}
