## Homework7

## 代码仓库链接：https://github.com/zhangyiyao020801/FBDP.git

### 1.设计思路

设计的目的是求出出现次数至少为min的含有k个元素的组合。

因此，对于每一行列出所有的含有k个元素的组合，进行map操作，在通过reduce操作进行求和，筛选出其中出现次数至少为min的组合。在最后的输出结果中，按组合出现次数从大到小，出现次数相同按组合字母序从小到大进行排列。

注意到每一行中可能会出现重复的元素，因此可以使用set先进行去重操作。接着进行一次排序，保证每个组合按字母大小顺序排列，这样可以规避出现（A，B）和（B，A）被认定为是两个不同组合的问题。

考虑列举出所有可能的含有k个元素的组合的方法，可以使用回溯递归的方法。伪代码如下所示：

```
k <- num of each combination
n <- items.size
combination <- []   // initial combination
ans <- []           // list of all combinations
backtrack(combination, 0, 0)

backtrack(combination, cnt, idx):
    if cnt == k:
        ans.push(combination)
        return
    if idx >= n:
        return
    ans.push(items[idx])
    backtrack(combination, cnt + 1, idx + 1)
    ans.pop()
    backtrack(combination, cnt, idx + 1)
```

### 2.输出结果

代码源文件保存在src/main/java/fis文件夹中。一共运行了四组结果，都保存在output/fis文件夹中。最终结果保存在对应文件夹中的final/part-r-00000中。

具体说明如下：

| 数据集     | k    | min  | 文件夹名称    |
| ---------- | ---- | ---- | ------------- |
| sample.txt | 2    | 2    | sample_k2min2 |
| MBA.txt    | 2    | 2    | MBA_k2min2    |
| MBA.txt    | 3    | 2    | MBA_k3min2    |
| MBA.txt    | 5    | 2    | MBA_k5min2    |


