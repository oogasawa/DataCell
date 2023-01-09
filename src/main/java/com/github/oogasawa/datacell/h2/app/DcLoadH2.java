package com.github.oogasawa.datacell.h2.app;

import com.github.oogasawa.datacell.app.DcLoad;
import com.github.oogasawa.datacell.h2.H2Factory;


public class DcLoadH2 extends DcLoad {
   
    public static void main(String[] args) {
        DcLoadH2 obj = new DcLoadH2();
        obj.run(args);        
    }
    

    public DcLoadH2() {
        super();
    }

    @Override
        public void initDCContainerFactory() {
        facObj = new H2Factory();
    }
    
}
