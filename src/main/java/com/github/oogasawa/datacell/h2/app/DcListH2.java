package com.github.oogasawa.datacell.h2.app;

import com.github.oogasawa.datacell.app.DcList;
import com.github.oogasawa.datacell.h2.H2Factory;


public class DcListH2 extends DcList {

    public static void main(String[] args) {
        DcListH2 obj = new DcListH2();
        obj.run(args);        
    }
    

    public DcListH2() {
        super();
    }

    @Override
    public void initDCContainerFactory() {
        facObj = new H2Factory();
    }

}
