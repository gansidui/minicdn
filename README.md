## MiniCDN
[![wercker status](https://app.wercker.com/status/38fd3ae3b11c17b6163966f06eb7a8be/m "wercker status")](https://app.wercker.com/project/bykey/38fd3ae3b11c17b6163966f06eb7a8be)

A simple CDN, based on [groupcache](https://github.com/golang/groupcache).

Use pull method, can't handle too big files. I limited to (512M) for now.

一般来说会推荐采用qiniu或者upyun,又或者是amazon之类大公司的cdn服务，不过当需要一些自己实现的场景，比如企业内部软件的加速，就需要一个私有的CDN了。

极简内容分发系统是我在公司里面的一个项目，最近把他开源出来了。可能其他企业或者组织也需要一个类似的东西。

通常来说CDN分为push和pull两种方式，push比较适合大文件，pull适合小一些的文件，但是使用起来比push要简单的多。

MiniCDN采用的就是pull这种方式，目前的实现方式是所有缓存的文件存储在内存中，使用LRU算法淘汰掉就的文件，镜像的文件受限于缓冲区的大小（目前的缓冲区是512M），如果超过了这个缓冲器大小，就没有加速的效果了。

没有所谓的智能DNS,直接用的是最简单的http redirect. 还没写负载均衡, 所以redirect的时候，就是随机返回一个节点（简单粗暴)

MiniCDN分为manager和peer。都是写在一个程序里。

我平常用的时候，就只开一个minicdn的Manager来加速我的后端服务器。如果没有节点的话，manager就会把自己当成一个节点。然后当有特别大的下载即将要冲击我的服务器的时候。我就会找很多的同事，将minicdn部署到他们平常用的电脑上(window系列, 因为是golang语言写的，什么平台的程序都能编译的出来)。这样我在短时间内就拥有了一个性能不错的cdn集群（充分利用同事的资源）。当下载冲击结束的时候，在把这些节点撤掉就可以了。相当省事

## 技术优势
MiniCDN使用了谷歌开源出来的groupcache框架，目前`dl.google.com`后台就用到了groupcache，性能而言远超那些squid或者nginx-proxy-cache.

groupcache的数据获取过程很有意思，我把他翻译了过来

**groupcache的运行过程**

[原文地址](https://github.com/golang/groupcache#loading-process)

查找`foo.txt`的过程(节点#5 是N个节点中的一个,每个节点的代码都是一样的)

1. 判断`foo.txt`是否内存中,并且很热门(super hot)，如果在就直接使用它
2. 判断`foo.txt`是否在内存中，并且当前节点拥有它(译者注:一致性hash查到该文件属于节点#5)，如果是就使用它
3. 在所有的节点中, 如果`foo.txt`的拥有者是节点#5，就加载这个文件。如果其他请求（通过直接的，或者rpc请求），节点#5会阻塞该请求，直接加载完毕，然后给所有请求返回同样的结果。否则使用rpc请求到拥有者的节点，如果请求失败，就本地加载(译者注:这种方式比较慢)

groupcache是2013年写出来的，软件也不怎么更新了。里面的HTTPPool还有两个问题一直没有修复，这两个问题直接影响到节点之间不能交换数据。因为官方不用groupcache的这部分，所以连用户提的issue都不修（真是蛋疼）

<https://github.com/codeskyblue/groupcache> 是我fork的，把这两个问题修复了，虽然提了pr，不过感觉他们一时半会不会merge的。

受python-celery的启发，我实现了peer退出时候的两种状态(Warm close and Code close). Warn close可以保证党节点不在服务的时候才退出。Code close就是强制退出，下载者可能会发现下载中断的问题。

## 架构

* M: Manager
	
	1. 负责维护Peer的列表,每个peer会去Manager同步这个列表。
	2. 所有的请求会先请求到manager, 然后由manager重定向到不同的peer

* P: Peer

	1. 提供文件的下载服务
	2. Peer之间会根据从manager拿到的peer列表，同步文件

Manager与Peer是一对多的关系

```
[M]
 |`------+--------+---......
 |       |        |
[P]     [P]      [P]  ....
```

## Installation
```go
go get -u -v github.com/codeskyblue/minicdn

# See help
minicdn -h
```

## Requirements
* First you need golang installed. [golang](https://golang.org/dl/)
* set env-var `GOPATH` for golang, ex: `export GOPATH=$HOME/goworkdir`, The target file will auto compile to `$GOPATH/bin`.

## Usage
minicdn contains two parts. manager and peer

### Run Manager

```shell
./minicdn -mirror http://localhost:5000 -addr :11000 -log cdn.log
```

* Speed up the mirror site: `http://localhost:5000`
* Listen address `:11000`
* Store log to `cdn.log`

The download resource link should change to this: `http://your-cdn-host:11000/something`

### Run Peer

```shell
./minicdn -upstream http://localhost:11000 -addr :8001
```

* Specify Manager address: `http://localhost:11000`
* Listen address `:8001`

### Log format

下载的请求的header中的两个头`X-Minicdn-Data`和`X-Minicdn-Type`两个字段，会被自动记录到后台的日志中。当`X-Minicdn-Type`设置为字符串`json`的时候，`X-Minicdn-Data`会相应的转化成json类型的数据。

**example**

```
{"data":{"action":"LOG","header_data":"","key":"/apks/com.dotgears.flappybird/com.dotgears.flappybird-4.apk","remote_addr":"10.242.189.222:55932","success":true,"user_agent":"AndroidDownloadManager/4.4.2 (Linux; U; Android 4.4.2; SM-G9006V Build/KOT49H)"},"peer":"http://10.246.14.51:19231","timestamp":1436945573}
{"data":{"action":"LOG","header_data":"","key":"/apks/com.netease.h15/com.netease.h15-1.apk","remote_addr":"10.242.189.222:39734","success":true,"user_agent":"AndroidDownloadManager/4.4.2 (Linux; U; Android 4.4.2; SM-G9006V Build/KOT49H)"},"peer":"http://10.246.14.51:19231","timestamp":1436949324}
```

When manager specified `-log`,
log can view through: `http://your-manager-host/_log`


### TODO
* token
* use a slave as a master
* request log
* cli args to specify cache size

## CONTRIBUTING
开源软件都会有一个这部分，感觉我也不能免俗。

* 代码或README中的拼写错误，都是可以提Pull request的。
* 如果有什么大的改动，请先提issue进行讨论。
* 代码使用golang默认的fmt

## Tests
```go
go test -v
```

## LICENSE
Under [MIT LICENSE](LICENSE)
