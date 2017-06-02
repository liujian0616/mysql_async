#include <mysql.h>
#include <stdint.h>
#include <assert.h>
#include <ev.h>
#include <sys/queue.h>
#include <map>
#include <string>
#include <stdio.h>
using namespace std;

#define util_offsetof(Type, Member) ( (size_t)( &(((Type*)8)->Member) ) - 8 )

class mconn;
class CResultSet;


struct mtask
{
    char sql[256];
    void *userdata;
    TAILQ_ENTRY(mtask) entry;       ///< tasklist管理入口
};

typedef TAILQ_HEAD(__mtask, mtask) mtask_list;  ///< 高效的双链管理

class handler
{
    public:
        virtual int on_execsql(mconn *c, mtask *task) = 0;
        virtual int on_query(mconn *c, mtask *task, CResultSet *result_set) = 0;
        virtual int on_close(mconn *c, mtask *task) = 0;
};


enum conn_state
{
    NO_CONNECTED = 1,
    CONNECT_WAITING,
    CONNECT_WRITE_ABLE,

    EXECSQL_WAITING,

    QUERY_WAITING,
    STORE_WAITING
};

class mconn
{
    public:
        mconn();
        ~mconn();

        static inline mconn* get_instance_via_watcher(ev_io* watcher)
        {   
            return reinterpret_cast<mconn*>( reinterpret_cast<uint8_t*>(watcher) - util_offsetof(mconn, m_watcher) );  
        }

        int init(char *ip, int port, char *user, char *passwd, char *dbname, handler *hand, struct ev_loop *loop);

        int event_to_mysql_status(int event);
        int mysql_status_to_event(int status);

        int listen_for_add_next_sql();

        int state_handle(struct ev_loop *loop, ev_io *watcher, int event);

        int connect_start();
        int connect_wait(struct ev_loop *loop, ev_io *watcher, int event);

        int execsql_start(char *sql, int sqllen);
        int execsql_wait(struct ev_loop *loop, ev_io *watcher, int event);

        int query_start(char *sql, int sqllen);
        int query_wait(struct ev_loop *loop, ev_io *watcher, int event);

        int store_result_start();
        int store_result_wait(struct ev_loop *loop, ev_io *watcher, int event);

        int close_start();
        int close_wait(struct ev_loop *loop, ev_io *watcher, int event);

        void add_task(mtask *task)
        {
            TAILQ_INSERT_TAIL(&m_mtask_list, task, entry);
        }

        void add_tasklist(mtask_list *tasklist)
        {
            TAILQ_CONCAT(&m_mtask_list, tasklist, entry);
        }

        void remove_task(mtask *task)
        {
            TAILQ_REMOVE(&m_mtask_list, task, entry);
        }

        mtask* fetch_task()
        {
            mtask *task = TAILQ_FIRST(&m_mtask_list);
            if (task != NULL)
                TAILQ_REMOVE(&m_mtask_list, task, entry);

            return task;
        }

        void printf_all_sql()
        {
            mtask *task = NULL;
            TAILQ_FOREACH(task, &m_mtask_list, entry)
            {
                printf("sql:%s\n", task->sql);
            }
        }


    private:
        char m_ip[32];
        int  m_port;
        char m_user[256];
        char m_passwd[256];
        char m_dbname[256];

        MYSQL m_mysql;
        MYSQL_RES *m_queryres;
        MYSQL_ROW row;

        handler *m_handler; 

        ev_io m_watcher;

        //ev_io m_rwatcher;
        //ev_io m_wwatcher;

        struct ev_loop *m_loop;

        mtask_list  m_mtask_list;
        conn_state m_state;
        mtask      *m_curtask;
};

class CResultSet
{
    public:
        CResultSet(MYSQL_RES *pstResultSet);
        ~CResultSet();

        /**
         * @fn void Close()
         * @brief 关闭结果集
         */
        void Close();

        /**
         * @fn int GetFieldNum()
         * @brief 获取结果集字段数
         * @return 结果集字段数
         */
        int GetColumnCount();

        /**
         * @fn int GetRowNum()
         * @brief 获取结果集行数
         * @return 结果集行数
         */
        int GetRecordCount();

        /**
         * @fn bool Next()
         * @brief 将游标移动到下一条记录
         * @return true - 成功; flase - 没有记录返回
         */
        bool Next();

        /**
         * @fn const char* GetFieldById(uint32_t dwFieldIndex)
         * @brief 以偏移方式获取指定字段的值
         * @return offset超过字段范围或者查询失败，则抛出异常
         */
        const char* GetField(uint32_t dwFieldIndex);

        /**
         * @fn const char* GetFieldByName(const char *psFieldName)
         * @brief 以偏移方式获取指定字段的值
         * @return offset超过字段范围或者查询失败，则抛出异常
         */
        const char* GetField(const char *psFieldName);
        /**
         * 
         * @fn const char* GetFieldName(uint32_t dwFieldIndex);
         * @breaf 以偏移方式获取字段名称
         * @return offset超过字段范围或者查询失败，则抛出异常
         */
        const char* GetFieldName(uint32_t dwFieldIndex);
        /**
         * @fn const char* GetLastErrMsg() const
         * @brief 获取最后错误描述
         * @return 最后错误描述
         */
        const char* GetLastErrMsg() const;

    private:
        MYSQL_RES *m_pstRusultSet;
        MYSQL_ROW  m_stCurrRow;
        char       m_szErrMsg[256];
        int        m_dwRowCount;
        int        m_dwFieldCount;
        std::map<std::string, int> m_vFieldNameToOffset;
        std::map<int, std::string> m_vFieldName;
        bool       m_bInitOk;
};
