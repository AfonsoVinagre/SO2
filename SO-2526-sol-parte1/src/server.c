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
#include <semaphore.h>
#include <signal.h>
#define CONTINUE_PLAY 0
#define NEXT_LEVEL 1
#define QUIT_GAME 2
#define LOAD_BACKUP 3
#define CREATE_BACKUP 4

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
    int notif_fd;
    int req_fd;
    int thread_shutdown;
    char input_command;
    pthread_mutex_t input_lock;
} session_t;
typedef struct {
    session_t* session;
    int ghost_index;
} ghost_thread_arg_t;


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
    char* arg_dir;
    session_t* session;
} client_args_t;
typedef struct {
    int max_levels;
    char *arg_dir;
} manager_thread_arg_t;
typedef struct {
    char* arg_dir;
    char* server_fifo;
    int max_levels;
} main_thread_arg_t;

session_t** buffer;
int in=0;
int out=0;
pthread_mutex_t buffer_lock;
sem_t empty;
sem_t full;
int active_clients = 0;
pthread_mutex_t active_clients_lock;
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
void send_board_update(board_t *board, int notif_fd,int victory,session_t *session) {
    if (!board) return;

    pthread_rwlock_rdlock(&board->state_lock);
    
    board_msg_t board_msg;
    board_msg.opcode = OP_UPDATE;
    board_msg.width = board->width;
    board_msg.height = board->height;
    board_msg.tempo = board->tempo;
    board_msg.game_over = board->pacmans[0].alive ? 0 : 1;
    board_msg.victory = victory;
    board_msg.points = board->pacmans[0].points;
    debug("POints: %d\n", board_msg.points);
    if (write_full(notif_fd, &board_msg, sizeof(board_msg_t)) != sizeof(board_msg_t)) {
        perror("write notif_pipe");
        pthread_rwlock_unlock(&board->state_lock);
        close(notif_fd);
        session->active = 0;
        session->thread_shutdown = 1;
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
    session_t *session = (session_t*) arg;
    board_t *board = session->board;

    pacman_t* pacman = &board->pacmans[0];

    int *retval = malloc(sizeof(int));

    while (true) {
        if (session->thread_shutdown) {
            *retval = QUIT_GAME;
            return (void*) retval;
        }
        if(!pacman->alive) {
            *retval = QUIT_GAME;
            return (void*) retval;
        }

        sleep_ms(board->tempo);
        if (session->thread_shutdown) {
            *retval = QUIT_GAME;
            return (void*) retval;
        }
        command_t* play;
        command_t c;
        if (pacman->n_moves == 0) {
            char cmd = '\0';
            pthread_mutex_lock(&session->input_lock);
            cmd = session->input_command;
            session->input_command = '\0';
            pthread_mutex_unlock(&session->input_lock);

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
        if (session->thread_shutdown) {
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
    session_t *session = ghost_arg->session;
    board_t *board = session->board;
    int ghost_ind = ghost_arg->ghost_index;

    free(ghost_arg);

    ghost_t* ghost = &board->ghosts[ghost_ind];

    while (true) {
        sleep_ms(board->tempo);

        pthread_rwlock_rdlock(&board->state_lock);
        if (session->thread_shutdown) {
            pthread_rwlock_unlock(&board->state_lock);
            pthread_exit(NULL);
        }
        
        move_ghost(board, ghost_ind, &ghost->moves[ghost->current_move%ghost->n_moves]);
        pthread_rwlock_unlock(&board->state_lock);
    }
}

void* periodic_update_thread(void* arg) {
    client_args_t* args = (client_args_t*)arg;
    session_t* session = args->session;
    board_t* board = session->board;
    while (!session->thread_shutdown) {
        sleep_ms(board->tempo);
        if (!session->thread_shutdown) {
            send_board_update(board, session->notif_fd,0, session);
        }
    }
    
    free(args);
    return NULL;
}
int count_levels(char* dirpath) {
   DIR *dir = opendir(dirpath);
    if (!dir) return -1;

    int count = 0;
    struct dirent *entry;

    while ((entry = readdir(dir)) != NULL) {
        if (entry->d_name[0] == '.')
            continue;

        char *dot = strrchr(entry->d_name, '.');
        if (dot && strcmp(dot, ".lvl") == 0) {
            count++;
        }
    }

    closedir(dir);
    return count;
}
void *pacman_input_thread(void *arg) {
    session_t* session = (session_t*)arg;
    while (session->active) {
    char opcode;
    ssize_t r = read_full(session->req_fd, &opcode, 1);

    if (r == 0) {
        printf("Client %d closed req pipe (disconnect)\n", session->id);
        close(session->notif_fd);
        close(session->req_fd);
        session->active = 0;
        session->thread_shutdown = 1;
        break;
    }

    if (r < 0) {
        perror("read req pipe");
        continue;
    }

    if (opcode == OP_PLAY) {
        char command;
        if (read_full(session->req_fd, &command, 1) == 1) {
            debug("Received command '%c' from client %d\n", command, session->id);
            pthread_mutex_lock(&session->input_lock);
            session->input_command = command;
            pthread_mutex_unlock(&session->input_lock);
        }
    } else if (opcode == OP_DISCONNECT) {
        debug("Client %d requested disconnect.\n", session->id);
        close(session->notif_fd);
        close(session->req_fd);
        session->active = 0;
        session->thread_shutdown = 1;
        break;
    }
}

    return NULL;
}
void *client_thread(void *arg) {
    client_args_t* args = (client_args_t*)arg;
    char* arg_dir = args->arg_dir;
    session_t* session = args->session;
    board_t* game_board = session->board;
    DIR* level_dir = opendir(arg_dir);
    if (level_dir == NULL) {
        fprintf(stderr, "Failed to open directory: %s\n", arg_dir);
        return 0;
    }
    struct dirent* entry;
    bool end_game = false;
    int accumulated_points = 0;
    int levels = count_levels(arg_dir);
    int current_level = 0;
    pthread_t inputtid;
    pthread_create(&inputtid, NULL, pacman_input_thread, session);
    while((entry = readdir(level_dir)) != NULL && !end_game) {
        if (entry->d_name[0] == '.') continue;
        char *dot = strrchr(entry->d_name, '.');
        if (!dot) continue;
        if (strcmp(dot, ".lvl") == 0) {
            load_level(game_board, entry->d_name, arg_dir, accumulated_points);
            current_level++;
            while(true) {
                pthread_t pacman_tid;
                pthread_t *ghost_tids = malloc(game_board->n_ghosts * sizeof(pthread_t));
                pthread_t update_tid;
                session->thread_shutdown = 0;
                pthread_create(&pacman_tid, NULL, pacman_thread_server, session);
                for (int i = 0; i < game_board->n_ghosts; i++) {
                    ghost_thread_arg_t *arg = malloc(sizeof(ghost_thread_arg_t));
                    arg->session = session;
                    arg->ghost_index = i;
                    pthread_create(&ghost_tids[i], NULL, ghost_thread_server, (void*) arg);
                }
                client_args_t* update_args = malloc(sizeof(client_args_t));
                update_args->session = session;
                pthread_create(&update_tid, NULL, periodic_update_thread, update_args);

                int *retval;
                pthread_join(pacman_tid, (void**)&retval);

                pthread_rwlock_wrlock(&game_board->state_lock);
                session->thread_shutdown = 1;
                pthread_rwlock_unlock(&game_board->state_lock);

                for (int i = 0; i < game_board->n_ghosts; i++) {
                    pthread_join(ghost_tids[i], NULL);
                }
                pthread_join(update_tid, NULL);
                free(ghost_tids);

                int result = *retval;
                free(retval);
                accumulated_points = game_board->pacmans[0].points; 
                if(result == NEXT_LEVEL) {
                    sleep_ms(game_board->tempo);
                    break;
                }
                if(result == QUIT_GAME) { 
                    sleep_ms(game_board->tempo);
                    end_game = true;
                    break;
                }  
            }
            if(current_level == levels && game_board->pacmans[0].alive) {
                send_board_update(game_board, session->notif_fd,1, session);
            }else{
                send_board_update(game_board, session->notif_fd,0, session);
            }
            unload_level(game_board);
        }
    }
    pthread_join(inputtid, NULL);
    closedir(level_dir);
    free(game_board);
    free(session);
    free(arg);
    pthread_mutex_lock(&active_clients_lock);
    active_clients--;
    pthread_mutex_unlock(&active_clients_lock);
    return NULL;
}
void *manager_thread(void *arg) {
    manager_thread_arg_t* args = (manager_thread_arg_t*) arg;
    char* arg_dir = args->arg_dir;
    int max_levels = args->max_levels;
    while (1) {
        sem_wait(&full);
        pthread_mutex_lock(&buffer_lock);
        session_t* session = buffer[out];
        out = (out + 1) % (max_levels);
        pthread_mutex_unlock(&buffer_lock);
        sem_post(&empty);

        client_args_t* cargs = malloc(sizeof(client_args_t));
        cargs->arg_dir = arg_dir; 
        cargs->session = session;
        pthread_t client_tid;
        if (pthread_create(&client_tid, NULL, client_thread, cargs) != 0) {
            perror("pthread_create");
            free(cargs);
            free(session);
            continue;
        }
        session->tid = client_tid;
    }
    return NULL;
}
void *main_thread(void *arg) {
    main_thread_arg_t* args = (main_thread_arg_t*) arg;
    char* arg_dir = args->arg_dir;
    char* server_fifo = args->server_fifo;
    int max_levels = args->max_levels;
    pthread_mutex_init(&buffer_lock, NULL);
    pthread_mutex_init(&active_clients_lock, NULL);
    buffer = malloc(max_levels * sizeof(session_t*));
    sem_init(&empty, 0, max_levels);
    sem_init(&full, 0, 0);

    if (mkfifo(server_fifo, 0666) == -1 && errno != EEXIST) {
        printf("Error creating server FIFO: %s\n", strerror(errno));
        perror("mkfifo");
        exit(1);
    }
    int server_fd = open(server_fifo, O_RDONLY | O_NONBLOCK);
    int dummy_fd  = open(server_fifo, O_WRONLY);
    if (server_fd < 0 || dummy_fd < 0) {
        printf("Error opening server FIFO: %s\n", strerror(errno));
        perror("open fifo");
        exit(1);
    }
    pthread_t manager_tids[max_levels];
    manager_thread_arg_t manager_args;
    manager_args.arg_dir = arg_dir;
    manager_args.max_levels = max_levels;
    for(int i=0;i<max_levels;i++){
        pthread_create(&manager_tids[i], NULL, manager_thread, &manager_args);
    }
    printf("Server ready. Waiting for clients...\n");
    int id=1;
    while (1) {
        char opcode;

        // Ler OP_CODE
        if (read_full(server_fd, &opcode, 1) <= 0) {
            continue;
        }

        if (opcode == OP_CONNECT) {
            connect_msg_t msg;
            msg.opcode = opcode;

            read_full(server_fd, msg.req_pipe, 40);
            read_full(server_fd, msg.notif_pipe, 40);

            int notif_fd = open(msg.notif_pipe, O_WRONLY);
            int req_fd = open(msg.req_pipe, O_RDONLY);
            if (notif_fd < 0) {
                printf("Error opening notification FIFO: %s\n", strerror(errno));
                perror("open notif fifo");
                continue;
            }
            pthread_mutex_lock(&active_clients_lock);

            while (active_clients == max_levels) {
                pthread_mutex_unlock(&active_clients_lock);
                sleep(1); 
                pthread_mutex_lock(&active_clients_lock);
            }

            active_clients++;

            pthread_mutex_unlock(&active_clients_lock);
            char reply[2];
            reply[0] = OP_CONNECT;
            reply[1] = id++;
            write_full(notif_fd, reply, 2);
            session_t* session = malloc(sizeof(session_t));
            session->active = 1;
            session->id = reply[1];
            session->notif_fd = notif_fd;
            session->req_fd = req_fd;
            session->thread_shutdown = 0;
            session->input_command = '\0';
            pthread_mutex_init(&session->input_lock, NULL);
            session->board = malloc(sizeof(board_t));
            pthread_rwlock_init(&session->board->state_lock, NULL);
            sem_wait(&empty);                    
            pthread_mutex_lock(&buffer_lock);
            buffer[in] = session;
            in = (in + 1) % max_levels;
            pthread_mutex_unlock(&buffer_lock);
            sem_post(&full);
        }
    }
    return NULL;
}
int main(int argc, char *argv[]) {
    open_debug_file("debug.log");
    signal(SIGPIPE, SIG_IGN);
    if (argc != 4) {
        printf("Usage: %s <levels_dir> <max_games> <server_fifo>\n",argv[0]);
        exit(1);
    }
    main_thread_arg_t main_args;
    main_args.arg_dir = argv[1];
    main_args.max_levels = atoi(argv[2]);
    main_args.server_fifo = argv[3];
    pthread_t main_tid;
    if (pthread_create(&main_tid, NULL, main_thread, &main_args) != 0) {
        perror("pthread_create");
        exit(1);
    }
    pthread_join(main_tid, NULL);
    
    return 0;
}

