
#include "mconn.h"
#include <stdio.h>

void libev_cb(struct ev_loop *loop, ev_io *watcher, int event)
{
    mconn *conn = mconn::get_instance_via_watcher(watcher);
    //printf("%s\n", __func__);

    conn->state_handle(loop, watcher, event);
}

mconn::mconn()
{
    TAILQ_INIT(&m_mtask_list);
}

mconn::~mconn()
{

}

int mconn::init(char *ip, int port, char *user, char *passwd, char *dbname, handler *hand, struct ev_loop *loop)
{

    m_state = NO_CONNECTED;
    strncpy(m_ip, ip, sizeof(m_ip));
    m_port = port;
    strncpy(m_user, user, sizeof(m_user));
    strncpy(m_passwd, passwd, sizeof(m_passwd));
    strncpy(m_dbname, dbname, sizeof(m_dbname));
    m_handler = hand;

    mysql_init(&m_mysql);
    mysql_options(&m_mysql, MYSQL_OPT_NONBLOCK, 0);

    m_loop = loop;

    connect_start();

    return 0;
}

int mconn::event_to_mysql_status(int event)
{
    int status= 0;
    if (event & EV_READ)
        status|= MYSQL_WAIT_READ;
    if (event & EV_WRITE)
        status|= MYSQL_WAIT_WRITE;
    return status;
}

int mconn::mysql_status_to_event(int status)
{
    int waitevent = 0;
    if (status & MYSQL_WAIT_READ)
    {
        waitevent |= EV_READ;
    }
    if (status & MYSQL_WAIT_WRITE)
    {
        waitevent |= EV_WRITE;
    }

    return waitevent;
}


int mconn::state_handle(struct ev_loop *loop, ev_io *watcher, int event)
{
    //printf("%s, state:%d\n", __func__, m_state);

    if (m_state == CONNECT_WAITING)
    {
        connect_wait(loop, watcher, event);
    }
    else if (m_state == EXECSQL_WAITING)
    {
        execsql_wait(loop, watcher, event);
    }
    else if (m_state == QUERY_WAITING)
    {
        query_wait(loop, watcher, event);
    }
    else if (m_state == STORE_WAITING)
    {
        store_result_wait(loop, watcher, event);
    }
    else if (m_state == CONNECT_WRITE_ABLE)
    {
        mtask* task = fetch_task();
        if (task != NULL)
        {
            m_curtask = task;
            if (strncmp(task->sql, "select", 6) == 0)
            {
                query_start(task->sql, strlen(task->sql));
            }
            else
            {
                execsql_start(task->sql, strlen(task->sql));
            }
        }

    }

    return 0;
}

int mconn::listen_for_add_next_sql()
{
    int socket = mysql_get_socket(&m_mysql);
    ev_io_init (&m_watcher, libev_cb, socket, EV_WRITE);
    ev_io_start(m_loop, &m_watcher);

    return 0;
}

int mconn::connect_start()
{
    MYSQL *ret;
    int status = mysql_real_connect_start(&ret, &m_mysql, m_ip, m_user, m_passwd, m_dbname, m_port, NULL, 0);
    if (status)
    {
        m_state = CONNECT_WAITING;

        int socket = mysql_get_socket(&m_mysql);
        int waitevent = mysql_status_to_event(status);
        ev_io_init (&m_watcher, libev_cb, socket, waitevent);
        ev_io_start(m_loop, &m_watcher);
        
    }
    else
    {
        m_state = CONNECT_WRITE_ABLE;
        listen_for_add_next_sql();

        //printf("connected, no need wait\n");
    }

    return 0;
}

int mconn::connect_wait(struct ev_loop *loop, ev_io *watcher, int event)
{
    MYSQL *ret;
    int status = event_to_mysql_status(event);

    status= mysql_real_connect_cont(&ret, &m_mysql, status);
    if (status != 0)
    {
        /*
         *@todo  LT模式会一直触发，等待下次被回调
         */
    }
    else
    {
        m_state = CONNECT_WRITE_ABLE;

        ev_io_stop(m_loop, &m_watcher);
        listen_for_add_next_sql();
        //printf("connected\n");
    }

    return 0;
}

int mconn::execsql_start(char *sql, int sqllen)
{
    int ret, status;
    status = mysql_real_query_start(&ret, &m_mysql, sql, sqllen);
    if (status)
    {
        m_state = EXECSQL_WAITING;

        int socket = mysql_get_socket(&m_mysql);
        int waitevent = mysql_status_to_event(status);
        ev_io_stop(m_loop, &m_watcher);
        ev_io_init (&m_watcher, libev_cb, socket, waitevent);
        ev_io_start(m_loop, &m_watcher);

    }
    else
    {

        //printf("execlsql done, no need wait\n");
        m_handler->on_execsql(this, m_curtask);

        m_state = CONNECT_WRITE_ABLE;

        ev_io_stop(m_loop, &m_watcher);
        listen_for_add_next_sql();
    }

    return 0;
}


int mconn::execsql_wait(struct ev_loop *loop, ev_io *watcher, int event)
{
    int ret = 0;
    int status = event_to_mysql_status(event);

    status= mysql_real_query_cont(&ret, &m_mysql, status);
    if (status != 0)
    {
        /*
         *@todo  LT模式会一直触发，等待下次被回调
         */
        m_state = EXECSQL_WAITING;
    }
    else
    {
        m_handler->on_execsql(this, m_curtask);

        m_state = CONNECT_WRITE_ABLE;

        ev_io_stop(m_loop, &m_watcher);
        listen_for_add_next_sql();
    }

    return 0;
}

int mconn::query_start(char *sql, int sqllen)
{
    int ret, status;
    status = mysql_real_query_start(&ret, &m_mysql, sql, sqllen);
    if (status)
    {
        m_state = QUERY_WAITING;

        int socket = mysql_get_socket(&m_mysql);
        int waitevent = mysql_status_to_event(status);

        ev_io_stop(m_loop, &m_watcher);
        ev_io_init (&m_watcher, libev_cb, socket, waitevent);
        ev_io_start(m_loop, &m_watcher);

    }
    else
    {
        //printf("query done, no need wait\n");
        store_result_start();
    }

    return 0;
}


int mconn::query_wait(struct ev_loop *loop, ev_io *watcher, int event)
{
    int ret = 0;
    int status = event_to_mysql_status(event);

    status= mysql_real_query_cont(&ret, &m_mysql, status);
    if (status != 0)
    {
        /*
         *@todo  LT模式会一直触发，等待下次被回调
         */
        m_state = QUERY_WAITING;
    }
    else
    {
        //printf("query done\n");

        store_result_start();
    }

    return 0;
}

int mconn::store_result_start()
{
    int status;
    status = mysql_store_result_start(&m_queryres, &m_mysql);
    if (status)
    {
        m_state = STORE_WAITING;

        int socket = mysql_get_socket(&m_mysql);
        int waitevent = mysql_status_to_event(status);
        ev_io_stop(m_loop, &m_watcher);
        ev_io_init (&m_watcher, libev_cb, socket, waitevent);
        ev_io_start(m_loop, &m_watcher);

    }
    else
    {

        //printf("store_result done, no need wait\n");
        CResultSet *result_set = new CResultSet(m_queryres);
        m_handler->on_query(this, m_curtask, result_set);


        m_state = CONNECT_WRITE_ABLE;

        ev_io_stop(m_loop, &m_watcher);
        listen_for_add_next_sql();
    }

    return 0;
}

int mconn::store_result_wait(struct ev_loop *loop, ev_io *watcher, int event)
{
    int status = event_to_mysql_status(event);

    status= mysql_store_result_cont(&m_queryres, &m_mysql, status);
    if (status != 0)
    {
        /*
         *@todo  LT模式会一直触发，等待下次被回调
         */
        m_state = STORE_WAITING;
    }
    else
    {


        //printf("store_result done\n");

        CResultSet *result_set = new CResultSet(m_queryres);
        m_handler->on_query(this, m_curtask, result_set);

        m_state = CONNECT_WRITE_ABLE;

        ev_io_stop(m_loop, &m_watcher);
        listen_for_add_next_sql();

    }

    return 0;
}

int mconn::close_start()
{
    int status;
    status = mysql_close_start(&m_mysql);
    if (status)
    {
        //int socket = mysql_get_socket(&m_mysql);
        //int waitevent = mysql_status_to_event(status);
        //ev_io_init (&m_watcher, close_cb, socket, waitevent);

        //ev_io_start(m_loop, &m_watcher);
        //ev_run(m_loop, 0);
    }
    else
    {
        //printf("close done, no need wait\n");
        //m_handler->on_close(this);
    }

    return 0;
}

int mconn::close_wait(struct ev_loop *loop, ev_io *watcher, int event)
{
    int status = event_to_mysql_status(event);

    status= mysql_close_cont(&m_mysql, status);
    if (status != 0)
    {
        /*
         *@todo  LT模式会一直触发，等待下次被回调
         */
    }
    else
    {
        //printf("close done\n");
        //ev_io_stop(loop, watcher);
        //m_handler->on_close(this);
    }

    return 0;
}


CResultSet::CResultSet(MYSQL_RES *m_pstRusultSet) :m_pstRusultSet(m_pstRusultSet)
{
    memset(m_szErrMsg, 0, sizeof (m_szErrMsg));
    m_stCurrRow = NULL;
    this->m_dwFieldCount = mysql_num_fields(m_pstRusultSet);
    this->m_dwRowCount   = mysql_num_rows(m_pstRusultSet);

    MYSQL_FIELD *fields = mysql_fetch_fields(m_pstRusultSet);
    for (int i = 0; i < this->m_dwFieldCount; ++i)
    {
        m_vFieldNameToOffset[fields[i].name] = i;
        m_vFieldName[i] = fields[i].name;
    }
}

CResultSet::~CResultSet()
{
    Close();
}

const char* CResultSet::GetLastErrMsg() const
{
    return m_szErrMsg;
}

void CResultSet::Close()
{
    mysql_free_result(m_pstRusultSet);
    m_pstRusultSet = NULL;
}

int CResultSet::GetColumnCount()
{
    assert(m_pstRusultSet);
    return m_dwFieldCount;
}

int CResultSet::GetRecordCount()
{
    assert(m_pstRusultSet);
    return m_dwRowCount;
}

bool CResultSet::Next()
{
    assert(m_pstRusultSet);
    m_stCurrRow = mysql_fetch_row(m_pstRusultSet);

    return (m_stCurrRow != NULL);
}

const char* CResultSet::GetField(uint32_t dwFieldIndex)
{
    assert(m_pstRusultSet);
    assert(m_stCurrRow);
    m_stCurrRow[dwFieldIndex] = (m_stCurrRow[dwFieldIndex] == NULL)?(char*)"":m_stCurrRow[dwFieldIndex];
    return m_stCurrRow[dwFieldIndex];
}

const char*  CResultSet::GetField(const char *psFieldName)
{
    assert(m_pstRusultSet);
    assert(m_stCurrRow);
    if (m_vFieldNameToOffset.find(psFieldName)!=m_vFieldNameToOffset.end())
    {
        int dwOffset = m_vFieldNameToOffset[psFieldName];
        m_stCurrRow[dwOffset] = (m_stCurrRow[dwOffset] == NULL)?(char*)"":m_stCurrRow[dwOffset];
        return m_stCurrRow[dwOffset];
    }
    return NULL;
}

const char* CResultSet::GetFieldName(uint32_t dwFieldIndex)
{
    return m_vFieldName[dwFieldIndex].c_str();
}
