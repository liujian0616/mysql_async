CC = gcc
CCC = g++
CFLAG = -g
LDFLAG = -g
INC= -I/usr/local/include/mariadb -I/usr/local/include
LIB= -L/usr/local/lib/mariadb -lmariadbclient  -L/usr/local/lib -lev -lssl

APP=test

OBJS = mconn.o test.o 

$(APP):$(OBJS)
	$(CCC) -o $@ $^ $(LIB)

%.o:%.cpp
	$(CCC) $(CFLAG) -c -o $@ $< $(INC)

%.o:%.c
	$(CC) $(CFLAG) -c -o $@ $< $(INC)

clean:
	@rm -rf $(OBJS)
	@rm -rf $(APP)
