set hive.execution.engine=tez;
---set tez.queue.name=serviceslob_jobs;
set tez.queue.name=serviceslob_adhoc;
set hivevar:TTAAWWHHDDBB2=pub_d8 ;
set hivevar:VVIIEEWWDDBB1=pub_d8 ;
set hivevar:TTMMPPDDBB1=pub_d8_work ;
set hivevar:AAPPLLIIDD1EENNVV=D8T ;
set hivevar:AAPPLLIIDD1=D8 ;
set hivevar:TTMMPPDDBB=pub_d8_work ;
set hivevar:EEDDWWDDBB1=pub_d8 ;
set hivevar:EEDDWWDDBB2=pub_d8 ;
set hivevar:EEDDWWDDBB=pub_d8;
set hivevar:eeddwwddbb=pub_d8;
set hivevar:PPDDAATTEE=30;
set hive.tez.container.size=8192;
set hive.tez.java.opts=-Xmx6550m; 
set tez.runtime.io.sort.mb=2047;  
set hivevar:EEDDWWDDBB3=pub_d8;
set hive.optimize.index.filter=false;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled=true;
set hive.vectorized.execution.reduce.groupby.enabled=true;
set hive.optimize.skewjoin=true;
set hive.optimize.skewjoin.compiletime=true;
set hive.cbo.enable=true;
set hive.compute.query.using.stats=true;



use pub_d8;
