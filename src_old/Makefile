TARGETS = sjf random hetergen
HPPFILES = tetrisched_constants.h TetrischedService.h tetrisched_types.h YARNTetrischedService.h
OBJS = tetrisched_constants.o TetrischedService.o tetrisched_types.o YARNTetrischedService.o
CC = g++
CFLAGS = -std=c++11 -Wall -Werror -DDEBUG -g # debug flags
#CFLAGS = -Wall -Werror -Os # release flags
LDFLAGS += -lthrift

default:	sjf
all:		$(TARGETS)

sjf:	$(OBJS) sjf.o
	$(CC) $(CFLAGS) -o schedpolserver $^ $(LDFLAGS)
	
random:	$(OBJS) random.o
	$(CC) $(CFLAGS) -o schedpolserver $^ $(LDFLAGS)
	
hetergen:	$(OBJS) hetergen.o
	$(CC) $(CFLAGS) -o schedpolserver $^ $(LDFLAGS)

%.o: %.cpp $(HPPFILES)
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	-rm schedpolserver *.o *.class

