package com.github.oogasawa.datacell.h2.app;

import com.github.oogasawa.datacell.app.DcDeleteTables;
import com.github.oogasawa.datacell.h2.H2Factory;


public class DcDeleteTablesH2 extends DcDeleteTables {

    public static void main(String[] args) {
        DcDeleteTablesH2 obj = new DcDeleteTablesH2();
        obj.run(args);        
    }
    

    public DcDeleteTablesH2() {
        super();
    }

    @Override
    public void initDCContainerFactory() {
        facObj = new H2Factory();
    }
    
}
