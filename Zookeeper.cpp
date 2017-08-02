/*
 * Describe: 对 Zookeeper 的封装，实现创建节点和节点监听功能
 * Author：LiuYang
 * Date: 2017-06-29
 * */
#include "Zookeeper.h"

int Zookeeper::st_mSessionState = ZOO_CONNECTED_STATE;
std::map<int, std::string>    Zookeeper::m_errorMsg;
Zookeeper::Zookeeper(void):m_pZkHandle(NULL),m_timeout(30000)
{
	//初始话错误代码
	setErrorCode();
}

Zookeeper::~Zookeeper(void)
{
	Destry();  //销毁资源
}

//初始化
int Zookeeper::Init(const char *host, void *context /*= NULL*/)
{
	if(m_pZkHandle != NULL)
	{
		//销毁之前初始化的资源
		Destry();
	}

	//初始化 zookeeper 句柄
	m_pZkHandle = zookeeper_init(host, session_watcher, m_timeout, NULL, context, 0);
	if(m_pZkHandle == NULL)
	{
		TRACE(LOG_ERROR, "Error when connection to zookeeper server.");
		return -1;
	}

	return 0;
}

//说明：监听节点状态
int Zookeeper::GetChildrenZoo(const char *path, watcher_fn watcher, void *watcherCtx, strings_completion_t completion, const void *data)
{
	if(m_pZkHandle == NULL)
	{
		TRACE(LOG_ERROR,"Error zkHandle is NULL.");
		return -1;
	}

	int ret = zoo_awget_children(m_pZkHandle, path, watcher, watcherCtx, completion, data);
	if(ret)
	{
		TRACE(LOG_ERROR, "Error when get children to zookeeper node. what: " << Zookeeper::GetErrorMsg(ret));
	}

	return ret;
}


//说明：监听节点状态
int Zookeeper::AwexistsZoo(const char *path, watcher_fn watcher, void *watcherCtx, stat_completion_t completion, const void *data)
{
	if(m_pZkHandle == NULL)
	{
		TRACE(LOG_ERROR,"Error zkHandle is NULL.");
		return -1;
	}
	int ret = zoo_awexists(m_pZkHandle, path, watcher, watcherCtx, completion, data);
	if(ret)
	{
		TRACE(LOG_ERROR, "Error when exists to zookeeper node. what: " << Zookeeper::GetErrorMsg(ret));
	}

	return ret;
}

//说明：创建 znode 节点
int Zookeeper::CreateZoo(const char *path, const char *value, int len, const ACLVector *acl,
							string_completion_t completion, const void *data, int flags/*= 0*/)
{
	if(m_pZkHandle == NULL)
	{
		TRACE(LOG_ERROR,"Error zkHandle is NULL.");
		return -1;
	}

	int ret = zoo_acreate(m_pZkHandle, path, value, len, acl, flags, completion, data);
	if(ret)
	{
		TRACE(LOG_ERROR, "Error when create to zookeeper node.what: " << Zookeeper::GetErrorMsg(ret));
	}

	return ret;
}

//说明：删除 zonde 节点
int Zookeeper::DeleteZoo(const char *path, void_completion_t completion, const void *data, int version /*= -1*/)
{
	if(m_pZkHandle == NULL)
	{
		TRACE(LOG_ERROR,"Error zkHandle is NULL.");
		return -1;
	}

	int ret = zoo_adelete(m_pZkHandle, path, version, completion, data);
	if(ret)
	{
		TRACE(LOG_ERROR, "Error when delete to zookeeper node. what: " << Zookeeper::GetErrorMsg(ret));
	}
	TRACE(LOG_DEBUG, "Zookeeper deleteZoo succesed.");

	return ret;
}

//说明：获取所有子节点路径 对 zoo_get_children的封装
int Zookeeper::GetChildrenPathZoo(const char *path, struct String_vector *children_path, zhandle_t *zh /*= NULL*/)
{
	int ret = 0;
	if(zh == NULL)
	{
		ret = zoo_get_children(m_pZkHandle, path, 1, children_path);
	}
	else
	{
		ret = zoo_get_children(zh, path, 1, children_path);
	}

	return 0;
}

//说明：获取指定节点的数据，对zoo_get的封装
int Zookeeper::GetZoo(const char *path, char *buf, int *len, struct Stat *stat, zhandle_t *zh /*= NULL*/)
{
	int ret = 0;
	if(zh == NULL)
	{
		ret = zoo_get(m_pZkHandle, path, 0, buf, len, stat);
	}
	else
	{
		ret = zoo_get(zh, path, 0, buf, len, stat);
	}

	return ret;
}

//说明：获取错误原因
std::string Zookeeper::GetErrorMsg(int err_num)
{
	return Zookeeper::m_errorMsg[err_num];
}

//销毁资源
int Zookeeper::Destry(void)
{
	if(m_pZkHandle == NULL)
	{
		TRACE(LOG_ERROR,"Error zkHandle is NULL.");
		return -1;
	}

	int ret = zookeeper_close(m_pZkHandle);
	if(ret)
	{
		TRACE(LOG_ERROR, "Error when close to zookeeper handle. what: " << Zookeeper::GetErrorMsg(ret));
	}
	m_pZkHandle = NULL;
	TRACE(LOG_DEBUG, "zookeeper Destry.");

	return ret;
}

//初始化错误代码
int Zookeeper::setErrorCode(void)
{
	Zookeeper::m_errorMsg[ZOK]                      = "ZOK";
	Zookeeper::m_errorMsg[ZSYSTEMERROR]             = "ZSYSTEMERROR";
	Zookeeper::m_errorMsg[ZRUNTIMEINCONSISTENCY]    = "ZRUNTIMEINCONSISTENCY";
	Zookeeper::m_errorMsg[ZDATAINCONSISTENCY]       = "ZDATAINCONSISTENCY";
	Zookeeper::m_errorMsg[ZCONNECTIONLOSS]          = "ZCONNECTIONLOSS";
	Zookeeper::m_errorMsg[ZMARSHALLINGERROR]        = "ZMARSHALLINGERROR";
	Zookeeper::m_errorMsg[ZUNIMPLEMENTED]           = "ZUNIMPLEMENTED";
	Zookeeper::m_errorMsg[ZOPERATIONTIMEOUT]        = "ZOPERATIONTIMEOUT";
	Zookeeper::m_errorMsg[ZBADARGUMENTS]            = "ZBADARGUMENTS";
	Zookeeper::m_errorMsg[ZINVALIDSTATE]            = "ZINVALIDSTATE";
	Zookeeper::m_errorMsg[ZAPIERROR]                = "ZAPIERROR";
	Zookeeper::m_errorMsg[ZNONODE]                  = "ZNONODE";
	Zookeeper::m_errorMsg[ZNOAUTH]                  = "ZNOAUTH";
	Zookeeper::m_errorMsg[ZBADVERSION]              = "ZBADVERSION";
	Zookeeper::m_errorMsg[ZNOCHILDRENFOREPHEMERALS] = "ZNODEEXISTS";
	Zookeeper::m_errorMsg[ZNOTEMPTY]                = "ZNOTEMPTY";
	Zookeeper::m_errorMsg[ZSESSIONEXPIRED]          = "ZSESSIONEXPIRED";
	Zookeeper::m_errorMsg[ZINVALIDCALLBACK]         = "ZINVALIDCALLBACK";
	Zookeeper::m_errorMsg[ZINVALIDACL]              = "ZINVALIDACL";
	Zookeeper::m_errorMsg[ZAUTHFAILED]              = "ZAUTHFAILED";
	Zookeeper::m_errorMsg[ZCLOSING]                 = "ZCLOSING";
	Zookeeper::m_errorMsg[ZNOTHING]                 = "ZNOTHING";
	Zookeeper::m_errorMsg[ZSESSIONMOVED]            = "ZSESSIONMOVED";
}

//全局的监控器回调函数
void Zookeeper::session_watcher(zhandle_t *zh, int type, int state, const char* path, void *watcherCtx)
{
	if( type == ZOO_SESSION_EVENT )
	{
		if( state == ZOO_EXPIRED_SESSION_STATE)
		{
			//会话结
			TRACE(LOG_DEBUG, "zookeeper session expred.");
			st_mSessionState = ZOO_EXPIRED_SESSION_STATE;
		}
	}
}

