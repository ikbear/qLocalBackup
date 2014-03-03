qLocalBackup
============

qiniu local Backup


##### 使用方法:   
`./backup -c <path to config file> {-s <port> | -p <key> | -b }  `

##### 参数含义:  

    -s <port> : 启动一个监听 <port> 端口的简易服务器，  
                通过请求 http://localhost:port/addkey?key=somekey 来新增文件记录;  
                
    -p <key>  : 新增一个文件记录  
    
    -b        : 开始备份  
    
    -v        : 详情模式  


##### config 格式:  
 
    {  
        "ips": [],  
        "bucket": "",  
        "domain": "",  
        "baseDir": "",  
        "accessKey": "",  
        "secretKey": ""  
    }
    
##### config 说明:

字段 | 必填 | 说明 | 范例  
:---- | :--- | :------------ | :------  
ips | 否 | 允许访问服务的ip列表，置为空或删除此行表示允许所有请求。 | `"ips":[]` 或 `"ips":["127.0.0.1"]`
bucket | 是 | 文件所处的存储空间名。 | `"bucket": "some-bucket"`
domain | 是 | 空间绑定的域名， 不含 `http://` 部分， 可在 `空间设置`-`域名设置` 中查看。 | `"domain": "some-bucket.qiniudn.com"`
baseDir | 是 | 备份工作的本地目录，日志与备份结果将分别保存在 `$baseDir/$bucket/log` 和 `$baseDir/$bucket/data` 中, windows用户请将路径中的`'\'`替换为`'/'`。 | `"baserDir": "/home/user/databackup"`
accessKey | 是 | 密钥组中的Access Key, 可以在 `账号设置`-`密钥` 中查看。 | `"accessKey": "iguImegxd6hbwF8J6ij2dlLIgycyU4thjg-xxxxx"`
secretKey | 是 | 密钥组中的Secret Key, 可以在 `账号设置`-`密钥` 中查看。 | `"accessKey": "ejqDluRblcUIW0ZIqP1gxxxxxxxxxxxxxxxxxxxx"`
 

