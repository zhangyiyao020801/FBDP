## Homework6

## 代码仓库链接：https://github.com/zhangyiyao020801/FBDP.git

### 1.设计思路

首先原始数据是一系列的二元组，分别表示一对对好友，不妨首先利用mapreduce对于每一个人给出他的所有好友。将一个人作为key，他的好友作为value，在reduce阶段得到每个key对应的所有好友。这一部分由源代码中的job1完成，得出的结果保存在相应文件夹中的step1中。

接下来，考察一个人的好友信息，比如：

1   2,3,4

我们可以发现(2,3), (2,4), (3,4)这三对两人组有共同好友1。于是可以给2推荐好友3和4，给3推荐好友2和4，给4推荐好友2和3，因为他们都有共同好友1。以给2推荐好友3，4为例，我们以2为key，设计一个二元组为value，在这里value标识为<3, 1>,<4, 1>，其中二元组的第一个元素表示给2推荐的好友，第二个元素表示他们的共同好友。但是，由于有些人之间本来就已经是好友，我们不需要再想他们推荐对方因此同样可以利用二元组记录这些关系，二元组的第一个元素表示他的好友，第二个元素为“yes”，表示他们已经是好友，以1为例，2，3，4已经是1的好友，我们便以1为key，得到3个value<2, "yes">,<3, "yes">,<4, "yes">。

以上是map阶段，下面在reduce阶段，对每一个固定的key，可以利用hashmap和set等内置数据结构，确定哪些还不是key的好友可以进行推荐（即二元组的第二个元素都不是“yes”）以及他们之间有哪些共同好友。

利用job2完成上述的mapreduce任务，FriendPair类描述了value对应的二元组数据结构，运行结果保存在对应文件夹的step2中，sample样例完成job2后的结果如下图所示。

<center><img src="https://s1.imagehub.cc/images/2022/10/26/2022-10-26-15.28.36.png" width="20%"></center>

然后，考虑排序问题。设计如下排序规则：先按共同好友数量从大到小排序，再按推荐好友的id大小从小到大排序，设计NewFriend类并灵活变换key和value完成排序任务。其中NewFriend中分别记录了共同好友的数量，推荐的好友的编号，共同好友序列等信息，重写sort函数完成排序。job3完成了排序，运行结果保存在对应文件夹的step3中。

最后job4将每个人所有推荐的好友按顺序合并在一起。最终结果保存在对应文件夹的final中，sample样例的最终结果如下所示。

<center><img src="https://s1.imagehub.cc/images/2022/10/26/2022-10-26-15.47.22.png" width="20%"></center>


### 2. 伪代码

由于job1对每个人列出所有好友，job3和job4完成排序，实现的功能较为简单，因此只对列出推荐好友的job2进行伪代码描述。

```
class Mapper
    procedure Map(id key, friend_list l)
        n <- l.size()

        // record possible recommended friends and shared friends
        for i = 0; i < n; ++i
            for j = i + 1; j < n; ++j
                emit(id l[i], pair <l[j], key>)
                emit(id l[j], pair <l[i], key>)

        // record friends
        for i = 0; i < n; ++i
            emit(id key, pair <l[i], "yes">)

class Reducer
    procedure Reduce(id key, pair [<n1, f1>, <n2, f2>, ...])
        set s <- {n1, n2, ...}
        for all n in s
            for all <n, f_n> in pair [<n1, f1>, <n2, f2>, ...]
                F_n = all f_n
                if all f_n != "yes"
                    // write information of recommended friend
                    emit(id key, n(F_n.size:[f_n1, f_n2, ...]))
```

### 3. 相关文件说明

源代码保存在src/main/java/fof/Friends.java，运行结果保存在output/fof中，sample和soc-pokec-relationships-small文件夹中分别是sample.txt和soc-pokec-relationships-small.txt中数据的运行结果。其中每个文件夹中包括job1，job2，job3，final，相关说明见设计思路。

注：由于soc-pokec-relationships-small运行结果较大，github仓库无法上传最后输出展示最终运行结果，在README中展示部分运行结果，运行结果截图如下所示。

<center><img src="https://s1.imagehub.cc/images/2022/10/28/2022-10-28-13.16.31.png" width="80%"></center>
