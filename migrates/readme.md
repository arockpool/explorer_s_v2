### 上线预处理
根目录下执行：

python3 ./migrate.py pre_process -s s1 -m explorer  

* -s：指定迭代号，即/data目录下的那个子目录将被执行
* -m：要处理的模块


### 上线预处理书写步骤及规范
* 每个迭代在/migrates目录下新增以迭代号命名的子目录；
* 需要预处理的服务在子目录下增加 pre_{service}.py 文件
* 参考 s1/pre_explorer.py 书写 PreProcess 类；
* 在 PreProcess 类中定义预处理方法，并在__init__方法中调用。



