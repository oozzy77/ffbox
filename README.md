## 📦 ffbox

Work in progress.

一种新式的**可以流式传输，边下载边运行，文件树别可复用的分布式存储的镜像格式**

**A better docker for deploying LLM or large AI model apps.** With our container fast streaming technology, you get instant inference, quick cold start time for your deployed LLM app.

Deploy your python AI app to cloud by just pushing your code to s3, and run it with a single command.

## Motivation

在花了几个月用各种云gpu平台部署API服务后，我发现用docker部署大模型是一件很痛苦的事，原因如下：

1️⃣大模型普遍size很大，如果将大模型打入docker镜像，那么镜像尺寸会很大20GB～100GB以上，导致push/pull都很慢，冷启动时间长，而且docker hub最大接受100GB的镜像

2️⃣不同docker镜像间无法共享/复用大模型，需要重复上传大模型，导致难以跨镜像缓存大模型，增加缓存成本
	
综上，提出一种新式的**可以流式传输，边下载边运行，文件树级别可复用的分布式存储的镜像格式**，适用于AI app打包。它的特性有：

✅无需等待整个镜像完整下载，懒加载文件，边运行边一个文件一个文件的下载镜像，加快冷启动

✅按需下载，只下载运行时需要的文件

✅不同镜像之间文件、文件夹都可以复用，类似docker image里的layer复用，更容易缓存

✅保存运行时文件被读取的顺序，提前多线程并行拉取文件，加快冷启动时间

✅可以直接复用huggingface上的模型文件到镜像，无需重复上传模型文件到镜像

✅镜像文件可以存储在任意对象存储如S3，COS里，可以自由设置
	
实现方式是基于FUSE实现一套虚拟文件系统作为镜像挂载，通过挂载路径访问镜像。本质上是把整个文件系统树存成一个元信息虚拟树里，这个metadata tree只存储文件元信息和指向的url。

<img width="900" alt="ffbox系统设计" src="https://github.com/user-attachments/assets/42b15011-0f94-4697-b00b-029efad08447" />

### Usage

Push local python project to s3 bucket

`cd my_python_project`

`ffbox push "s3://my_bucket/my_python_project"`

Pull s3 bucket to local directory

`ffbox pull "s3://my-bucket/flux_image_gen"`

Run python project

`ffbox run "s3://my-bucket/flux_image_gen"`

### Benchmarks

no cache in ffbox_cache/ cold start - Pillow image processing:

`ffbox run "s3://ff-image-gen/sd3"` - 11s

