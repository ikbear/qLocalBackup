qLocalBackup
============

qiniu local Backup

#####
使用方法:   
./backup -c \<path to config file\> {-s \<port\> | -p \<key\> | -b }  
#####  
参数含义:  
-s \<port\> : 启动一个监听 \<port\> 端口的简易服务器，  
            通过请求 http://localhost:port/key?key=somekey 来新增文件记录;  
-p \<key\>  : 新增一个文件记录  
-b        : 开始备份  
-v        : 详情模式  
#####  
config 格式:   
{  
    "ips": "",  
    "bucket": "",  
    "domain": "",  
    "baseDir": "",  
    "accessKey": "",  
    "secretKey": ""  
}

