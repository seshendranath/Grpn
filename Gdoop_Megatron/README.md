tungsten-utils
==============

o CDH5 - 

- Added Megatron Monitoring and Notification task, 
   daily summary
   which sends out an hourly report if there is an error  

  - Allows us to quickly identify problems and fix them
  - this morning one table sync started failing on test cluster: deal_localized_contents
  
- Adding Megatron status page to web server page- Prabakaran
  - this will include link to fail log files that will help trouble shoot problem

- Tungsten License Renewal - Cleared up Open Source licensing issue. 
   -> Eliminated fee based commercial license
   -> Negotiate Adding Postgres support 

- Neelesh has additional tables - about 20 that he wants added to megatron
   -  Will test a 30 minute sync cycle
   - Source : About 7 from a mysql server that we are currently replicating
   - others from new mysql servers

  
- Zombie Runner - start design of plugin architecture
   Review Charles LibraryTask feature designed by c
   
   
 GOODS:
   si-pomanager-db2.snc1
   goods-shipment-db-master-vip.snc1
   goods-receiver-db2.snc1
   goods-outbound-controller-db2.snc1