all settings in conf.py  
entery point run.py  

You need to create db conf.py:db_name manually:  
```CREATE DATABASE es_parser;```
  
How dump works:  
change date in conf.py:dumper.dump_date to needed date  
recreate docker iamge, if you use it or k8s and run dump yaml  
or run python3 dump_elastic_data.py  
watch logs for line like this:  
"2020-06-11 15:22:11,504 - es-parser - INFO - Dump 157146 items complete"   
get dump folder  
example for k8s:  
```bash  
kubectl exec -it es-parser-dump-5fcc5cffd7-f8jls -n kube-logging /bin/bash  
tar -czvf dump.tar.gz dump_XXXX  
exit  
kubectl cp es-parser-dump-5fcc5cffd7-f8jls:dump.tar.gz -n kube-logging ./dump.tar.gz  
```  
to work with dump, you'll need postgres, you can use docker:  
```docker run --name postgres -d -p 5432:5432 -e POSTGRES_PASSWORD=ndlKuberCul57#dev postgres:alpine```  
  