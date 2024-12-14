# 讲解内容

![image-20241214085831648](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20241214085831648.png)

PM：项目经理，向各部门传递需求  BA：产品经理，向dev传递需求

业务部只管公司的利润，数据中台属于成本，服务于业务部

如果公司裁员，在数据中台里面优先裁AI部门里面的人，成本高难得到变现，其次是分析部门，分析部门的工作开发部门也可以做，然后是BA产品经理，部门可以直接向开发人员传递需求

# 今日进度

#### 	1、编写Dim_App类，将数据从mysql数据库读取到flink后，进行封装实体类

#### 		代码：

![image-20241213163205378](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20241213163205378.png)

#### 		效果：

![image-20241213163256751](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20241213163256751.png)

#### 	任务进行过程中遇到的问题/报错：

##### 			无

#### 	2、随后进行在hbase中，根据配置表创建表

#### 		代码：

![image-20241213163413448](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20241213163413448.png)

### 	同时借助工具类HBaseUtil

#### 		效果：

![image-20241213163726995](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20241213163726995.png)

#### 	任务进行过程中遇到的问题/报错：

##### 		第一次运行时创建表失败，结果发现没有创建命名空间（create_namespace）

#### 	3、将主流和广播流进行连接

#### 		代码：

![image-20241213164140597](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20241213164140597.png)

#### 		效果：

![image-20241213164111286](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20241213164111286.png)

#### 	任务进行过程中遇到的问题/报错：

##### 			无

#### 	4、将维度表数据进行过滤，过滤掉不需要的字段

#### 		代码：

![image-20241213162919395](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20241213162919395.png)

#### 		效果：

![image-20241213162932200](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20241213162932200.png)

#### 	任务进行过程中遇到的问题/报错：

##### 		无