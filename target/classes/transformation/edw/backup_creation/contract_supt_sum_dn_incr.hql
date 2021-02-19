Set hive.exec.compress.output='false';
Set hive.execution.engine=tez;
Set hive.tez.container.size=16384;
Set hive.tez.java.opts=-Xmx13106m;
set tez.queue.name=${hivevar:queueName};

use ${hivevar:targetSchema};

------------------------------------------------------------------------------
ALTER TABLE CONTRACT_SUPT_SUM_DN_NEW RENAME TO CONTRACT_SUPT_SUM_DN_PREVIOUS; 



