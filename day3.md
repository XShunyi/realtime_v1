# 数据倾斜

##### **1、思路：**

​	第一步（数据转换），先将地区转换，在地区字段后面添加了一个分隔符，然后添加了一个随机数，确保没有重复性

​	第二步（数据聚合），读取t1的数据，根据转化过后的新地区字段分组，求出每个地区的数量，会发现所有数量都为1 ，没有重复性

​	第三步（再次处理），读取t2的数据，再将新地区字段进行切割，只获取需要的，再次求数量，但不分组

​	第四步（最终结果），读取t3的数据，根据切割后的字段进行分组，再计算每个地区的数量

##### **2、代码实现：**

with t1 as (    

select userid,           

​	order_no,           

​	**concat**(region,'-',**rand**()) as region,           

​	product_no,           

​	color_no,           

​	sale_amount,           

​	ts    

from date_east ),    t2 

as (        

select 

​	region,               

​	**count**(1) as cnt        

from t1        

group by region    ),    t3 

as (        

select region,               

​	**substr**(region,1,2) as re,               

​	cnt        

from t2    )

select 

​	re,       

​	count**(1) as cnt 

from t3 

group by re;

# 今日进度

##### 首先，成功搭建了ods层和dim层，并且测试了flinkcdc，通过flink读取mysql数据库和kafka中的数据	然后将业务数据从kafka中读取到之后封装实体类

##### 1、测试使用了flinkcdc的正常性，通过flink代码读取了mysql数据库的数据

###### 		代码：

![image-20241213092310795](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20241213092310795.png)

###### 		效果：

![image-20241213092342422](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20241213092342422.png)

##### 2、使用flink读取kafka的业务数据

（首先需要执行jar包，将数据导入到mysql数据库里面，然后通过Maxwell将数据从mysql数据库导入到kafka中，最后用flink读取kafka的数据）

#### 		步骤：

##### 	（1）：执行jar包

![image-20241213093530340](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20241213093530340.png)

##### 	（2）：查看数据库数据并且查看kafka消费者

#### 		![image-20241213093712459](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20241213093712459.png)

#### 		代码：

![image-20241213092819123](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20241213092819123.png)

#### 		效果：

![image-20241213093733618](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20241213093733618.png)