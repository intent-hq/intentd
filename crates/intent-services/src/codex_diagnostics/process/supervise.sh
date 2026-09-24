# Linux marks this process as a subreaper before exec. The extra descriptors
# carry private lifetime/status traffic; all provider stdio remains unchanged.
exec 3<"/proc/self/fd/$1"
exec 4>"/proc/self/fd/$2"
shift 2
trap ':' TERM INT HUP
# A non-interactive shell replaces descriptor 0 for background commands.
# Preserve the provider input before that happens and restore it explicitly.
exec 5<&0
# Keep one stable first child that never forks. Waiting for it lets the shell
# reap adopted children instead of leaving them zombies in a blocking read.
while IFS= read -r request <&3; do :; done 0<&- 1>&- 2>&- 4>&- 5<&- &
control=$!
"$@" <&5 >&1 2>&2 3<&- 4>&- 5<&- &
probe=$!
# No further forks occur after this startup acknowledgment.
printf '%010d\n' "$control" >&4
exec 0<&- 1>&- 2>&- 5<&-
wait "$probe"
status=$?
printf '%03d\n' "$status" >&4
exec 4>&-
wait "$control"
