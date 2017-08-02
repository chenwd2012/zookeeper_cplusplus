/*
 * Describe: 对 Zookeeper 的封装，实现创建节点和节点监听功能
 * Author：LiuYang
 * Date: 2017-06-29
 * Other: 关于 Zookeeper C API 相关资料请参考：http://www.cnblogs.com/haippy/archive/2013/02/21/2920280.html
 * */

#ifndef __ZOOKEEPER_H__
#define __ZOOKEEPER_H__

#include <string>
#include <map>
#include <zookeeper/zookeeper.h>
#include <zookeeper/zookeeper_log.h>

//#include "logTools.h"

typedef struct ACL_vector ACLVector;

//st_mSessionState = ZOO_SESSION_STATE;
class Zookeeper
{
public:
	Zookeeper(void);
	~Zookeeper(void);

	//初始化
	//参数：host：zookeeper服务器地址和ip "ip:port,ip:port"
	//返回：0 成功
	int Init(const char *host, void *context = NULL);

	//说明：监控字节点状态
	//参数：
	//	path:       节点路径
	//	watcher:    如果非 NULL, 则在服务器端设置监视，当监视节点发生变化或被创建时调用该函数
	//	watcherCtx: 用户指定数据，将被传入到回调函数中
	//	completion: 当请求完成时，会调用该函数,同时传给 completion 的参数为：
	//		    	ZOK 操作完成;
	//		    	ZNONODE 节点不存在;
	//		    	ZHOAUTH 客户端没有权限操作该节点;
	//	data:       completion 函数被调用时，传递给的数据。
	//返回：结果代码
	int GetChildrenZoo(const char *path, watcher_fn watcher, void *watcherCtx, strings_completion_t completion, const void *data);

	//说明：监听节点状态
	//参数： 
	//	path:       节点路径
	//	watcher:    如果非 NULL, 则在服务器端设置监视，当监视节点发生变化或被创建时调用该函数
	//	watcherCtx: 用户指定数据，将被传入到回调函数中
	//	completion: 当请求完成时，会调用该函数,同时传给 completion 的参数为：
	//		    	ZOK 操作完成;
	//		    	ZNONODE 节点不存在;
	//		    	ZHOAUTH 客户端没有权限操作该节点;
	//	data:       completion 函数被调用时，传递给的数据。
	//返回：结果代码
	int AwexistsZoo(const char *path, watcher_fn watcher, void *watcherCtx, stat_completion_t completion, const void *data);

	//说明：创建 znode 节点
	//参数：
	//	path:       节点路径
	//	value:      该节点保存的数据
	//	len:        所保存数据的长度
	//	acl:        该节点初始 ACL，不能为 NULL 或空
	//	completion: 当请求完成时，会调用该函数,同时传给 completion 的参数为：
	//		    	ZOK 操作完成;
	//		    	ZNONODE 父节点不存在
	//		    	ZNODEEXISTS 节点以存在
	//		    	ZNOAUTH 没有权限
	//		    	ZNOCHILDRENFOREPHEMERALS 临时节点不能创建子节点
	//	data:       completion 函数被调用时，传递给的数据。
	//	flags:      默认为0， 可以为: ZOO_EPHEMERAL, ZOO_SEQUENCE 的组合或(OR)
	//返回：结果代码
	int CreateZoo(const char *path, const char *value, int len, const ACL_vector *acl,
								string_completion_t completion, const void *data, int flags = 0);

	//说明：删除 zonde 节点
	//参数：
	//	path:     节点路径
	//	completion: 当请求完成时，会调用该函数,同时传给 completion 的参数为：
	//			ZOK 操作完成；
	//			ZNONODE 节点不存在；
	//			ZNOAUTH 客户端没有权限删除节点；
	//			ZBADVERSION 版包号不匹配；
	//			ZNOTEMPTY 当前节点存在子节点，不能被删除。
	//	data:       completion 函数被调用时，传递给的数据。
	//	version:  期望删除的版本号，如果真实的版本好不同则删除识别， 默认为-1 标识不检查版本号
	//返回：结果代码
	int DeleteZoo(const char *path, void_completion_t completion, const void *data, int version = -1);

	//说明：获取所有子节点路径 对 zoo_get_children的封装
	//参数：
	//	path： 父节点路径
	//	children_path: 返回参数，返回子节点路径和子节点数量
	//			children_path.count: 子节点数量，children_data[N]：子节点名称
	//	zh：zookeeper 句柄，默认为NULL，当为NULL 时使用 zookeeper_init() 返回的 句柄。
	//返回：结果代码
	int GetChildrenPathZoo(const char *path, struct String_vector *children_path, zhandle_t *zh = NULL);

	//说明：获取指定节点的数据，对zoo_get的封装
	//参数：
	//	path： 父节点路径
	//	buf:   返回参数，返回节点数据
	//	len：  返回参数，返回数据长度
	//	stat：返回参数，节点状态
	//	zh：zookeeper 句柄，默认为NULL，当为NULL 时使用 zookeeper_init() 返回的 句柄。
	//返回：结果代码
	int GetZoo(const char *path, char *buf, int *len, struct Stat *stat, zhandle_t *zh = NULL);

	//说明：获取错误原因
	//参数： err_num zookeeper c aip 返回的错误码
	//返回：返回 zookeeper c aip 定义的错误宏
	static std::string GetErrorMsg(int err_num);
private:
	//销毁资源
	int Destry(void);

	//初始化错误代码
	int setErrorCode(void);
private:
	//全局的监控器回调函数
	static void session_watcher(zhandle_t *zh, int type, int state, const char* path, void *watcherCtx);
private:
	zhandle_t                    *m_pZkHandle;           //Zookeeper 句柄
	int                           m_timeout;             //超时
private:
	static std::map<int, std::string>    m_errorMsg;            //错误信息
	static int                           st_mSessionState;      //会话状态
};

#endif //__ZOOKEEPER_H__
