#include "mconn.h"
#include <stdio.h>

class my_handler : public handler
{
    public:

        virtual int on_execsql(mconn *c, mtask *task)
        {
            printf("%s\n", __func__);
            printf("sql: %s\n", task->sql);
            //char sql[] = "select * from test.test where s1=2";
            //c->query_start(sql, strlen(sql));
            return 0;
        }

        virtual int on_query(mconn *c, mtask *task, CResultSet *result_set)
        {
            printf("%s\n", __func__);
            printf("sql: %s\n", task->sql);

            printf("%8s%8s\n", "id", "s1");
            while (result_set->Next())
            {
                printf("%8s%8s\n", result_set->GetField("id"), result_set->GetField("s1"));
            }

            sleep(10);

            //c->close_start();
            return 0;
        }

        virtual int on_close(mconn *c, mtask *task)
        {
            printf("%s\n", __func__);
            return 0;
        }

};

#if 1
int main()
{
    char ip[16] = "192.168.1.150";
    int port = 3306;
    char user[256] = "ec";
    char passwd[256] = "ecEC!)@(#*$*";
    char dbname[256] = "test";

    struct ev_loop *loop = EV_DEFAULT;
    mconn conn1;
    my_handler hand;
    conn1.init(ip, port, user, passwd, dbname, &hand, loop);

#if 1
    struct mtask taskarr[3];
    for (int i = 0; i < 2; i++)
    {
        char sql[256] = {0};
        snprintf(sql, 256, "insert into test.test(s1) values(%d)", i);
        //printf("%s\n", sql);
        strncpy(taskarr[i].sql, sql, 256);

        conn1.add_task(&taskarr[i]);
    }
#endif

    struct mtask task1;
    strcpy(task1.sql, "select * from test.test where s1 = 0");
    task1.userdata = NULL;
    
    conn1.add_task(&task1);

    conn1.printf_all_sql();


    while (1)
    {
        ev_run(loop, 0);
    }

    return 0;
}
#endif
