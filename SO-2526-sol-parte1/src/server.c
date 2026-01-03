#include "board.h"
#include "display.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>
#include <sys/wait.h>
#include <pthread.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/select.h>

#define CONTINUE_PLAY 0
#define NEXT_LEVEL 1
#define QUIT_GAME 2
#define LOAD_BACKUP 3
#define CREATE_BACKUP 4

typedef struct {
    board_t *board;
    int ghost_index;
} ghost_thread_arg_t;


#define OP_CONNECT     1
#define OP_DISCONNECT  2
#define OP_PLAY        3
#define OP_UPDATE      4

typedef struct {
    char opcode;          
    char req_pipe[40];
    char notif_pipe[40];
} connect_msg_t;
typedef struct {
  char opcode;
  char command;
} play_msg_t;
typedef struct {
    int id;
    int active;
    pthread_t tid;
    board_t* board;
    char req_pipe[40];
    char notif_pipe[40];
    int notif_fd;
    int req_fd;
} session_t;
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
    board_t *board;
    int notif_fd;
    char* arg_dir;
} client_args_t;

#define CMD_QUEUE_SIZE 16

typedef struct {
    pthread_mutex_t lock;
    char queue[CMD_QUEUE_SIZE];
    int head, tail, count;
} pacman_input_t;

pacman_input_t pacman_input = {
    .lock = PTHREAD_MUTEX_INITIALIZER,
    .queue = {0},
    .head = 0,
    .tail = 0,
    .count = 0
};


int thread_shutdown_server = 0;
session_t session;
ssize_t read_full(int fd, void *buf, size_t size) {
    size_t total = 0;
    while (total < size) {
        ssize_t r = read(fd, (char*)buf + total, size - total);
        if (r <= 0) return r;
        total += r;
    }
    return total;
}

ssize_t write_full(int fd, const void *buf, size_t size) {
    size_t total = 0;
    while (total < size) {
        ssize_t w = write(fd, (char*)buf + total, size - total);
        if (w <= 0) return w;
        total += w;
    }
    return total;
}
void send_board_update(board_t *board, int notif_fd) {
    if (!board) return;

    pthread_rwlock_rdlock(&board->state_lock);
    
    board_msg_t board_msg;
    board_msg.opcode = OP_UPDATE;
    board_msg.width = board->width;
    board_msg.height = board->height;
    board_msg.tempo = board->tempo;
    board_msg.game_over = board->pacmans[0].alive ? 0 : 1;
    board_msg.victory = 0;
    board_msg.points = board->pacmans[0].points;

    if (write_full(notif_fd, &board_msg, sizeof(board_msg_t)) != sizeof(board_msg_t)) {
        pthread_rwlock_unlock(&board->state_lock);
        perror("write notif_pipe");
        return;
    }

    size_t board_size = board_msg.width * board_msg.height;
    char* data = malloc(board_size);

    for (size_t i = 0; i < board_size; i++) {
        char content;
        if(board->board[i].has_dot) {
            content = '.';
        } else if(board->board[i].has_portal) {
            content = '@';
        } else if(board->board[i].content == 'W') {
            content = '#';
        } else {
            content = ' ';
        }
        data[i] = content;
    }

    for(int g = 0; g < board->n_ghosts; g++) {
        ghost_t* ghost = &board->ghosts[g];
        if (ghost->pos_x >= 0 && ghost->pos_x < (int)board->width &&
            ghost->pos_y >= 0 && ghost->pos_y < (int)board->height) {
            size_t pos = ghost->pos_y * board->width + ghost->pos_x;
            data[pos] = ghost->charged ? 'G' : 'M';
        }
    }

    for(int p = 0; p < board->n_pacmans; p++) {
        pacman_t* pacman = &board->pacmans[p];
        if (pacman->alive && 
            pacman->pos_x >= 0 && pacman->pos_x < (int)board->width &&
            pacman->pos_y >= 0 && pacman->pos_y < (int)board->height) {
            size_t pos = pacman->pos_y * board->width + pacman->pos_x;
            data[pos] = 'C';
        }
    }

    pthread_rwlock_unlock(&board->state_lock);
    
    write_full(notif_fd, data, board_size);
    free(data);
}
void* pacman_thread_server(void *arg) {
    board_t *board = (board_t*) arg;

    pacman_t* pacman = &board->pacmans[0];

    int *retval = malloc(sizeof(int));

    while (true) {
        if (thread_shutdown_server) {
            *retval = QUIT_GAME;
            return (void*) retval;
        }
        if(!pacman->alive) {
            *retval = LOAD_BACKUP;
            return (void*) retval;
        }

        sleep_ms(board->tempo * (1 + pacman->passo));
        if (thread_shutdown_server) {
            *retval = QUIT_GAME;
            return (void*) retval;
        }
        command_t* play;
        command_t c;
        if (pacman->n_moves == 0) {
            char cmd = '\0';
            pthread_mutex_lock(&pacman_input.lock);
            if (pacman_input.count > 0) {
                cmd = pacman_input.queue[pacman_input.head];
                pacman_input.head = (pacman_input.head + 1) % CMD_QUEUE_SIZE;
                pacman_input.count--;
            }
            pthread_mutex_unlock(&pacman_input.lock);

            if (cmd == '\0') {
                continue;
            }
            c.command = cmd;
            c.turns = 1;
            play = &c;
        } else {
            play = &pacman->moves[pacman->current_move % pacman->n_moves];
        }

        debug("KEY %c\n", play->command);

        // QUIT
        if (play->command == 'Q') {
            *retval = QUIT_GAME;
            return (void*) retval;
        }

        pthread_rwlock_rdlock(&board->state_lock);
        if (thread_shutdown_server) {
            pthread_rwlock_unlock(&board->state_lock);
            *retval = QUIT_GAME;
            return (void*) retval;
        }
        int result = move_pacman(board, 0, play);
        if (result == REACHED_PORTAL) {
            // Next level
            *retval = NEXT_LEVEL;
            break;
        }
        if(result == DEAD_PACMAN) {
            // Restart from child, wait for child, then quit
            *retval = QUIT_GAME;
            break;
        }
        pthread_rwlock_unlock(&board->state_lock);
    }
    pthread_rwlock_unlock(&board->state_lock);
    return (void*) retval;
}

void* ghost_thread_server(void *arg) {
    ghost_thread_arg_t *ghost_arg = (ghost_thread_arg_t*) arg;
    board_t *board = ghost_arg->board;
    int ghost_ind = ghost_arg->ghost_index;

    free(ghost_arg);

    ghost_t* ghost = &board->ghosts[ghost_ind];

    while (true) {
        sleep_ms(board->tempo * (1 + ghost->passo));

        pthread_rwlock_rdlock(&board->state_lock);
        if (thread_shutdown_server) {
            pthread_rwlock_unlock(&board->state_lock);
            pthread_exit(NULL);
        }
        
        move_ghost(board, ghost_ind, &ghost->moves[ghost->current_move%ghost->n_moves]);
        pthread_rwlock_unlock(&board->state_lock);
    }
}

void* periodic_update_thread(void* arg) {
    client_args_t* args = (client_args_t*)arg;
    board_t* board = args->board;
    int notif_fd = args->notif_fd;
    send_board_update(board, notif_fd);
    while (!thread_shutdown_server) {
        sleep_ms(board->tempo);  // Enviar update a cada tempo
        if (!thread_shutdown_server) {
            send_board_update(board, notif_fd);
        }
    }
    
    free(args);
    return NULL;
}
void *client_thread(void *arg) {
    client_args_t* args = (client_args_t*)arg;
    board_t* game_board = args->board;
    char* arg_dir = args->arg_dir;
    int notif_fd = args->notif_fd;
    DIR* level_dir = opendir(arg_dir);
    if (level_dir == NULL) {
        fprintf(stderr, "Failed to open directory: %s\n", arg_dir);
        return 0;
    }

    struct dirent* entry;
    bool end_game = false;
    int accumulated_points = 0;
    while((entry = readdir(level_dir)) != NULL && !end_game) {
        if (entry->d_name[0] == '.') continue;
        char *dot = strrchr(entry->d_name, '.');
        if (!dot) continue;
        if (strcmp(dot, ".lvl") == 0) {
            load_level(game_board, entry->d_name, arg_dir, accumulated_points);
            send_board_update(game_board, notif_fd);
            while(true) {
                pthread_t pacman_tid;
                pthread_t *ghost_tids = malloc(game_board->n_ghosts * sizeof(pthread_t));
                pthread_t update_tid;
                thread_shutdown_server = 0;

                debug("Creating threads\n");

                pthread_create(&pacman_tid, NULL, pacman_thread_server, (void*) game_board);
                for (int i = 0; i < game_board->n_ghosts; i++) {
                    ghost_thread_arg_t *arg = malloc(sizeof(ghost_thread_arg_t));
                    arg->board = game_board;
                    arg->ghost_index = i;
                    pthread_create(&ghost_tids[i], NULL, ghost_thread_server, (void*) arg);
                }
                client_args_t* update_args = malloc(sizeof(client_args_t));
                update_args->board = game_board;
                update_args->notif_fd = notif_fd;
                pthread_create(&update_tid, NULL, periodic_update_thread, update_args);

                int *retval;
                pthread_join(pacman_tid, (void**)&retval);

                pthread_rwlock_wrlock(&game_board->state_lock);
                thread_shutdown_server = 1;
                pthread_rwlock_unlock(&game_board->state_lock);

                for (int i = 0; i < game_board->n_ghosts; i++) {
                    pthread_join(ghost_tids[i], NULL);
                }
                pthread_join(update_tid, NULL);
                free(ghost_tids);

                int result = *retval;
                free(retval);

                if(result == NEXT_LEVEL) {
                    sleep_ms(game_board->tempo);
                    break;
                }
                if(result == QUIT_GAME) { 
                    sleep_ms(game_board->tempo);
                    end_game = true;
                    break;
                }

                accumulated_points = game_board->pacmans[0].points; 
                send_board_update(game_board, notif_fd);     
            }
            unload_level(game_board);
        }
    }
    if(!end_game) {
        board_msg_t board_msg;
        board_msg.opcode = OP_UPDATE;
        board_msg.width = game_board->width;
        board_msg.height = game_board->height;
        board_msg.tempo = game_board->tempo;
        board_msg.game_over = 0;
        board_msg.victory = 1; 
        board_msg.points = game_board->pacmans[0].points;

        write_full(notif_fd, &board_msg, sizeof(board_msg_t));
    }
    free(arg);
    closedir(level_dir);
    return NULL;
}
int main(int argc, char *argv[]) {
    int id=1;
    if (argc != 4) {
        printf("Usage: %s <levels_dir> <max_games> <server_fifo>\n",argv[0]);
        exit(1);
    }

    char *server_fifo = argv[3];
    open_debug_file("debug.log");
    printf("Server FIFO: %s\n", server_fifo);
    // Criar FIFO do servidor
    if (mkfifo(server_fifo, 0666) == -1 && errno != EEXIST) {
        printf("Error creating server FIFO: %s\n", strerror(errno));
        perror("mkfifo");
        exit(1);
    }
    printf("Server FIFO created.\n");
    // Abrir FIFO (truque anti-EOF)
    int server_fd = open(server_fifo, O_RDONLY | O_NONBLOCK);
    int dummy_fd  = open(server_fifo, O_WRONLY);
    printf("Server FIFO opened.\n");
    if (server_fd < 0 || dummy_fd < 0) {
        printf("Error opening server FIFO: %s\n", strerror(errno));
        perror("open fifo");
        exit(1);
    }

    printf("Server ready. Waiting for clients...\n");
    while (1) {
        char opcode;

        // Ler OP_CODE
        if (read_full(server_fd, &opcode, 1) <= 0) {
            continue;
        }

        if (opcode == OP_CONNECT) {
            connect_msg_t msg;
            msg.opcode = opcode;

            // Ler nomes dos FIFOs
            read_full(server_fd, msg.req_pipe, 40);
            read_full(server_fd, msg.notif_pipe, 40);

            printf("New client:\n");
            printf("  req_fifo  = %s\n", msg.req_pipe);
            printf("  notif_fifo= %s\n", msg.notif_pipe);

            // Abrir FIFO de notificações do cliente
            int notif_fd = open(msg.notif_pipe, O_WRONLY);
            int req_fd = open(msg.req_pipe, O_RDONLY);
            printf("Notification FIFO opened.\n");
            if (notif_fd < 0) {
                printf("Error opening notification FIFO: %s\n", strerror(errno));
                perror("open notif fifo");
                continue;
            }

            // Enviar resposta (sucesso)
            char reply[2];
            reply[0] = OP_CONNECT;
            reply[1] = id++;  // sucesso
            printf("Sending client ID %d\n", reply[1]);
            write_full(notif_fd, reply, 2);
            printf("Client connected with ID %d\n", reply[1]);
            session.active = 1;
            session.id = reply[1];
            strncpy(session.req_pipe, msg.req_pipe, 40);
            strncpy(session.notif_pipe, msg.notif_pipe, 40);
            session.notif_fd = notif_fd;
            session.req_fd = req_fd;
            board_t *board = malloc(sizeof(board_t));
            pthread_rwlock_init(&board->state_lock, NULL);
            session.board = board; 

            client_args_t* args = malloc(sizeof(client_args_t));
            args->board = board;  // Passa o mesmo ponteiro
            args->arg_dir = argv[1];
            args->notif_fd = notif_fd;
            pthread_t client_tid;
            if (pthread_create(&client_tid, NULL, client_thread, args) != 0) {
                perror("pthread_create");
                free(args);
                continue;
            }
            session.tid = client_tid;
            break;
        }
    }
    while (!thread_shutdown_server) {
        if(session.active) {
            fd_set readfds;
            FD_ZERO(&readfds);
            FD_SET(session.req_fd, &readfds);
            
            struct timeval timeout;
            timeout.tv_sec = 0;
            timeout.tv_usec = 100000; // 100ms
            
            int ret = select(session.req_fd + 1, &readfds, NULL, NULL, &timeout);
            
            if (ret < 0) {
                perror("select");
                break;
            }
            
            if (ret == 0) {
                // Timeout - sem dados, continuar o loop
                continue;
            }
            
            // Há dados para ler
            if (FD_ISSET(session.req_fd, &readfds)) {
                char opcode;
                ssize_t r = read_full(session.req_fd, &opcode, 1);
                debug("Server read opcode %d from client %d\n", opcode, session.id);
                
                if (r == 0) {
                    printf("Client %d closed req pipe (disconnect)\n", session.id);
                    close(session.notif_fd);
                    close(session.req_fd);
                    session.active = 0;
                    thread_shutdown_server = 1;
                    pthread_join(session.tid, NULL);
                    free(session.board);
                    continue; 
                }
                if (r < 0) {
                    perror("read req pipe");
                    continue;
                }
                
                if(opcode == OP_PLAY) {
                    char command;
                    if(read_full(session.req_fd, &command, 1) == 1) {
                        debug("Received command '%c' from client %d\n", command, session.id);
                        pthread_mutex_lock(&pacman_input.lock);
                        if (pacman_input.count < CMD_QUEUE_SIZE) {
                            pacman_input.queue[pacman_input.tail] = command;
                            pacman_input.tail = (pacman_input.tail + 1) % CMD_QUEUE_SIZE;
                            pacman_input.count++;
                        }
                        pthread_mutex_unlock(&pacman_input.lock);
                    }
                }else if(opcode == OP_DISCONNECT) {
                    debug("Client %d requested disconnect.\n", session.id);
                    close(session.notif_fd);
                    close(session.req_fd);
                    session.active = 0;
                    thread_shutdown_server = 1;
                    pthread_join(session.tid, NULL);
                    free(session.board);
                    printf("Client %d disconnected.\n", session.id);
                }
            }
        }
    }

    return 0;
}

