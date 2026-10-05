#define _GNU_SOURCE
/* Test-only preload: create an escaped shell descendant and deny only its
 * TERM/KILL signals. The outer Rust test, without this preload, cleans it up. */
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
static pid_t sentinel_pid(void) {
  const char *p=getenv("INTENT_VERIFY_REAP_SENTINEL");
  if (!p) return 0;
  FILE *f=fopen(p,"r"); if (!f) return 0;
  int pid=0; if (fscanf(f,"%d",&pid)!=1) pid=0; fclose(f); return pid;
}
int kill(pid_t pid, int sig) {
  static int (*real_kill)(pid_t,int);
  if (!real_kill) real_kill=dlsym(RTLD_NEXT,"kill");
  if (pid>1 && pid==sentinel_pid() && (sig==SIGTERM||sig==SIGKILL)) {
    errno=EPERM; return -1;
  }
  return real_kill(pid,sig);
}
__attribute__((constructor)) static void create_sentinel(void) {
  const char *marker=getenv("INTENT_VERIFY_REAP_SENTINEL");
  if (!marker) return;
  char exe[4096]; ssize_t n=readlink("/proc/self/exe",exe,sizeof(exe)-1);
  if(n<0) return; exe[n]=0;
  const char *base=strrchr(exe,'/'); base=base?base+1:exe;
  if(strcmp(base,"dash")&&strcmp(base,"bash")) return;
  if(sentinel_pid()>1) return;
  pid_t child=fork(); if(child!=0) return;
  setsid(); signal(SIGTERM,SIG_IGN);
  int fd=open(marker,O_WRONLY|O_CREAT|O_TRUNC,0600);
  if(fd>=0) {dprintf(fd,"%d",getpid()); close(fd);}
  close(0);close(1);close(2);
  for(;;) pause();
}
