#include "api.h"
#include "protocol.h"
#include "debug.h"

#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <errno.h>


struct Session {
  int id;
  int req_pipe;
  int notif_pipe;
  char req_pipe_path[MAX_PIPE_PATH_LENGTH + 1];
  char notif_pipe_path[MAX_PIPE_PATH_LENGTH + 1];
};
typedef struct {
    char opcode;          
    char req_pipe[40];
    char notif_pipe[40];
} connect_msg_t;
typedef struct {
  char opcode;
  int width;
  int height;
  int tempo;
  int game_over;
  int victory;
  int points;
}board_msg_t;
typedef struct {
  char opcode;
  char command;
} play_msg_t;
static struct Session session = {.id = -1};
ssize_t read_full(int fd, void *buf, size_t size) {
    size_t total = 0;

    while (total < size) {
        ssize_t r = read(fd, (char*)buf + total, size - total);

        if (r > 0) {
            total += r;
            continue;
        }
        if (r == 0) {
            return 0;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            usleep(1000);  
            continue;
        }
        if (errno == EINTR) {
            continue;  
        }
        return -1;
    }

    return total;
}

int pacman_connect(char const *req_pipe_path, char const *notif_pipe_path, char const *server_pipe_path) {
  if(mkfifo(req_pipe_path,0666)==-1){
    perror("mkfifo req_pipe");
    unlink(req_pipe_path);
    printf("Failed to create req pipe\n");
    return 1;
  }
  if(mkfifo(notif_pipe_path,0666)==-1){
    perror("mkfifo notif_pipe");
    unlink(req_pipe_path);
    unlink(notif_pipe_path);
    printf("Failed to create notif pipe\n");
    return 1;
  }
  int notif_fd = open(notif_pipe_path, O_RDONLY | O_NONBLOCK);
  if(notif_fd==-1){
    perror("open notif_pipe");
    unlink(req_pipe_path);
    unlink(notif_pipe_path);
    printf("Failed to open notif pipe\n");
    return 1;
  }
  int server_pipe = open(server_pipe_path,O_WRONLY);
  if(server_pipe==-1){
    perror("open server_pipe");
    unlink(req_pipe_path);
    unlink(notif_pipe_path);
    close(server_pipe);
    printf("Failed to open server pipe\n");
    return 1;
  }
  connect_msg_t msg;
  msg.opcode=OP_CODE_CONNECT;
  strncpy(msg.req_pipe,req_pipe_path,40);
  strncpy(msg.notif_pipe,notif_pipe_path,40);
  msg.req_pipe[39] = '\0';
  msg.notif_pipe[39] = '\0';
  if(write(server_pipe,&msg,sizeof(connect_msg_t))!=sizeof(connect_msg_t)){
    perror("write server_pipe");
    unlink(req_pipe_path);
    unlink(notif_pipe_path);
    close(server_pipe);
    printf("Failed to write to server pipe\n");
    return 1;
  }
  session.req_pipe = open(req_pipe_path,O_WRONLY);
  session.notif_pipe = notif_fd;
  strncpy(session.req_pipe_path, req_pipe_path, MAX_PIPE_PATH_LENGTH);
  strncpy(session.notif_pipe_path, notif_pipe_path, MAX_PIPE_PATH_LENGTH);
  unsigned char reply[2];
  ssize_t r;

  for (;;) {
      r = read(session.notif_pipe, reply, 2);
      if (r == 2)
          break;
      if (r == -1 && errno == EAGAIN)
        continue;
      perror("read notif_pipe");
      return 1;
  }
  if (r != 2 || reply[0] != OP_CODE_CONNECT) {
    perror("read notif_pipe");
    unlink(req_pipe_path);
    unlink(notif_pipe_path);
    close(server_pipe);
    printf("Failed to read from notif pipe\n");
    return 1;
  }
  session.id = reply[1];
  close(server_pipe);
  return 0;
}

int pacman_play(char command) {
  if(session.id == -1) {
      debug("Not connected to server, cannot send play command\n");
      return -1;
  }
  play_msg_t msg;
  msg.opcode = OP_CODE_PLAY;
  msg.command = command;
  if(write(session.req_pipe, &msg, sizeof(play_msg_t))!=sizeof(play_msg_t)){
    perror("write req_pipe");
    debug("Failed to send play command\n");
    return -1;
  }
  return 0;
}

int pacman_disconnect(void) {
  if (session.id == -1)
      return 1;

  char opcode = OP_CODE_DISCONNECT;
  if (write(session.req_pipe, &opcode, 1) != 1) {
      perror("write req_pipe");
      debug("Failed to send disconnect request\n");
      return 1;
  }

  close(session.req_pipe);
  close(session.notif_pipe);

  unlink(session.req_pipe_path);
  unlink(session.notif_pipe_path);

  session.id = -1;
  session.req_pipe = -1;
  session.notif_pipe = -1;

  debug("Disconnected from server\n");
  return 0;
}


Board receive_board_update(void) {
    Board board;
    board.data = NULL;
    if (session.id == -1) {
        debug("Session not active, returning empty board\n");
        return board;
    }

    board_msg_t msg;
    ssize_t bytes_read = read_full(session.notif_pipe, &msg, sizeof(board_msg_t));
    
    if (bytes_read == 0) {
        debug("Server closed notification pipe (EOF)\n");
        return board;
    }
    
    if (bytes_read != sizeof(board_msg_t)) {
        perror("read notif_pipe header");
        debug("Failed to read board update header (got %zd bytes)\n", bytes_read);
        return board;
    }

    board.width = msg.width;
    board.height = msg.height;
    board.tempo = msg.tempo;
    board.game_over = msg.game_over;
    board.victory = msg.victory;
    board.accumulated_points = msg.points;

    size_t board_size = board.width * board.height;
    board.data = malloc(board_size);
    if (!board.data) {
        debug("Failed to allocate board data\n");
        return board;
    }
    
    bytes_read = read_full(session.notif_pipe, board.data, board_size);
    
    if (bytes_read == 0) {
        debug("Server closed pipe during board data read\n");
        free(board.data);
        board.data = NULL;
        return board;
    }
    
    if (bytes_read != (ssize_t)board_size) {
        perror("read notif_pipe data");
        debug("Failed to read board update data (got %zd of %zu bytes)\n", bytes_read, board_size);
        free(board.data);
        board.data = NULL;
        return board;
    }

    return board;
}