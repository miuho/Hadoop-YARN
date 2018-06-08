TARGETS = soft none hard
HPPFILES = tetrisched_constants.h TetrischedService.h tetrisched_types.h YARNTetrischedService.h
OBJS = tetrisched_constants.o TetrischedService.o tetrisched_types.o YARNTetrischedService.o
CC = g++
CFLAGS = -std=c++11 -Wall -Werror -DDEBUG -g # debug flags
#CFLAGS = -Wall -Werror -Os # release flags
LDFLAGS += -lthrift

default:	soft
all:		$(TARGETS)

soft:	$(OBJS) soft.o
	$(CC) $(CFLAGS) -o schedpolserver $^ $(LDFLAGS)
	
none:	$(OBJS) none.o
	$(CC) $(CFLAGS) -o schedpolserver $^ $(LDFLAGS)
	
hard:	$(OBJS) hard.o
	$(CC) $(CFLAGS) -o schedpolserver $^ $(LDFLAGS)

%.o: %.cpp $(HPPFILES)
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	-rm schedpolserver *.o *.class
