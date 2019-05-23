# hbase-obsever-elasticsearch
hbase数据同步到elasticsearch
hbase1.x
es6.3.2

使用Hbase协作器(Coprocessor)同步数据到ElasticSearch
最近项目中需要将Hbase中的数据同步到ElasticSearch中，需求就是只要往Hbase里面put或者delete数据，那么ES集群中，相应的索引下，也需要更新或者删除这条数据。本人使用了hbase-rirver插件，发现并没有那么好用，于是到网上找了一些资料，自己整理研究了一下，就自己写了一个同步数据的组件，基于Hbase的协作器，效果还不错，现在共享给大家，如果大家发现什么需要优化或者改正的地方，可以在我的csdn博客：我的csdn博客地址上面私信我给我留言，代码托管在码云上Hbase-Observer-ElasticSearch。同时要感谢Gavin Zhang 2shou，我虽然不认识Gavin Zhang 2shou，（2shou的同步数据博文）但是我是看了他写的代码以及博客之后，（2shou的同步组件代码）在他的基础之上对代码做了部分优化以及调整，来满足我本身的需求，所以在此表示感谢，希望我把我的代码开源出来，其他人看到之后也能激发你们的灵感，来写出更多更好更加实用的东西：

Hbase协作器(Coprocessor)
编写组件
部署组件
验证组件
总结
##Hbase协作器(Coprocessor)

HBase 0.92版本后推出了Coprocessor — 协处理器，一个工作在Master/RegionServer中的框架，能运行用户的代码，从而灵活地完成分布式数据处理的任务。

HBase 支持两种类型的协处理器，Endpoint 和 Observer。Endpoint 协处理器类似传统数据库中的存储过程，客户端可以调用这些 Endpoint 协处理器执行一段 Server 端代码，并将 Server 端代码的结果返回给客户端进一步处理，最常见的用法就是进行聚集操作。如果没有协处理器，当用户需要找出一张表中的最大数据，即 max 聚合操作，就必须进行全表扫描，在客户端代码内遍历扫描结果，并执行求最大值的操作。这样的方法无法利用底层集群的并发能力，而将所有计算都集中到 Client 端统一执行，势必效率低下。利用 Coprocessor，用户可以将求最大值的代码部署到 HBase Server 端，HBase 将利用底层 cluster 的多个节点并发执行求最大值的操作。即在每个 Region 范围内执行求最大值的代码，将每个 Region 的最大值在 Region Server 端计算出，仅仅将该 max 值返回给客户端。在客户端进一步将多个 Region 的最大值进一步处理而找到其中的最大值。这样整体的执行效率就会提高很多。
另外一种协处理器叫做 Observer Coprocessor，这种协处理器类似于传统数据库中的触发器，当发生某些事件的时候这类协处理器会被 Server 端调用。Observer Coprocessor 就是一些散布在 HBase Server 端代码中的 hook 钩子，在固定的事件发生时被调用。比如：put 操作之前有钩子函数 prePut，该函数在 put 操作执行前会被 Region Server 调用；在 put 操作之后则有 postPut 钩子函数。
**在实际的应用场景中，第二种Observer Coprocessor应用起来会比较多一点，因为第二种方式比较灵活，可以针对某张表进行绑定，假如hbase有十张表，我只想绑定其中的5张表，另外五张不需要处理，就不绑定即可，下面我要介绍的也是第二种方式。 **
编写组件
###验证组件

hbase shell
create 'test_record','info'

disable 'test_record'

alter 'test_record', METHOD => 'table_att', 'coprocessor' => 'hdfs:///hbase_es/hbase-observer-elasticsearch-1.0-SNAPSHOT-zcestestrecord.jar|org.eminem.hbase.observer.HbaseDataSyncEsObserver|1001|es_cluster=zcits,es_type=zcestestrecord,es_index=zcestestrecord,es_port=9100,es_host=master'

enable 'test_record'

put 'test_record','test1','info:c1','value1'
deleteall 'test_record','test1'
绑定操作之前需要，在ES集群中建立好相应的索引以下是对绑定代码的解释: 把Java项目打包为jar包，上传到HDFS的特定路径 进入HBase Shell，disable你希望加载的表 通过alert 命令激活Observer coprocessor对应的格式以|分隔，依次为：

jar包的HDFS路径

Observer的主类

优先级（一般不用改）

参数（一般不用改）

新安装的coprocessor会自动生成名称：coprocessor + $ + 序号（通过describe table_name可查看）

以后对jar包内容做了调整，需要重新打包并绑定新jar包，再绑定之前需要做目标表做解绑操作，加入目标表之前绑定了同步组件的话，以下是解绑的命令

hbase shell

disable 'test_record'
alter 'test_record', METHOD => 'table_att_unset',NAME => 'coprocessor$1'
enable 'test_record'
desc 'test_record'
总结
*绑定之后如果在执行的过程中有报错或者同步不过去，可以到hbase的从节点上的logs目录下，查看hbase-roor-regionserver-slave.log文件。因为协作器是部署在regionserver上的，所以要到从节点上面去看日志，而不是master节点。 **

**hbase-river插件之前下载了源代码看了下，hbase-river插件是周期性的scan整张表进行bulk操作，而我们这里自己写的这个组件呢，是基于hbase的触发事件来进行的，两者的效果和性能不言而喻，一个是全量的，一个是增量的，我们在实际的开发中，肯定是希望如果有数据更新了或者删除了，我们只要对着部分数据进行同步就行了，没有修改或者删除的数据，我们可以不用去理会。 **

Timer和 ScheduledExecutorService，在这里我选择了ScheduledExecutorService，2shou之前提到过部署插件有个坑，修改Java代码后，上传到HDFS的jar包文件必须和之前不一样，否则就算卸载掉原有的coprocessor再重新安装也不能生效，这个坑我也碰到了，就是因为没有复写stop方法，将定时任务停掉，线程一直会挂在那里，而且一旦报错将会导致hbase无法启动，必须要kill掉相应的线程。这个坑，坑了我一段时间，大家千万要注意，一定记得要复写stop方法，关闭之前打开的线程或者客户端，这样才是最好的方式。
