package com.github.oogasawa.datacell.h2.app;

import com.github.oogasawa.datacell.app.DcJoin;
import com.github.oogasawa.datacell.h2.H2Factory;


public class DcJoinH2 extends DcJoin {
    
        public static void main(String[] args) {
        DcJoinH2 obj = new DcJoinH2();
        obj.run(args);        
    }
    

    public DcJoinH2() {
        super();
    }

    @Override
    public void initDCContainerFactory() {
        facObj = new H2Factory();
    }
    
}
