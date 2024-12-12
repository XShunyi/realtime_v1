# **数据生成**

### 日志数据：

###### 	1、先执行jar包，将数据发送到app.log中

![image-20241211112925117](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20241211112925117.png)

###### 	2、通过flume采集 发送到kafka

![image-20241211113826772](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20241211113826772.png)



### 业务数据：

###### 	1、执行jar包将数据发送到mysql里面

![image-20241211115025949](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20241211115025949.png)

###### 	2、使用maxwell将数据发送到kafka

**![image-20241211133953252](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20241211133953252.png)**

###### 	3、用flink读取kafka中的数据

需要单独创建一个配置表，不能在gmall数据库里面创建

![image-20241211203446095](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20241211203446095.png)