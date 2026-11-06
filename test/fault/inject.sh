#!/bin/bash
# Start a hyperion worker, or simulate its loss before it connects. Used as
# HYPERION_SCHEDULER_TEST_WORKER_WRAPPER; see README.md.
#
#   FAULT_DIR        folder for the launch counter and the record of faults (required)
#   FAULT_KIND       empty: no fault. One of:
#                      exit1   the process exits at once with an error
#                      vanish  an empty log, then exit 0: nothing says why (bootstrap-compute job 55544734)
#                      hang    the process lives and never connects
#                      badport the real worker runs, but its messages to the master go to a channel that
#                              doesn't exist: it logs "Couldn't connect to master" and exits (job 55556183)
#   FAULT_LOG_RE     hit only workers whose --logFile matches this
#   FAULT_PARENT_RE  and whose parent's --logFile matches this (optional)
#   FAULT_AT         the first matching launch to hit, from 1 (default 1)
#   FAULT_COUNT      how many matching launches in a row to hit (default 1)
set -u
: "${FAULT_DIR:?}"
mkdir -p "$FAULT_DIR"
args=("$@") logfile="" service=""
for ((i = 0; i < ${#args[@]}; i++)); do
  case ${args[i]} in
    --logFile) logfile=${args[i + 1]} ;;
    --service) service=${args[i + 1]} ;;
  esac
done
kind=${FAULT_KIND:-}
parent_ok=1
if [[ -n ${FAULT_PARENT_RE:-} ]]; then
  parent_log=$(tr '\0' '\n' < "/proc/$PPID/cmdline" 2> /dev/null | grep -A1 -x -- --logFile | tail -1)
  [[ $parent_log =~ $FAULT_PARENT_RE ]] || parent_ok=0
fi
if [[ -n $kind && $parent_ok == 1 && $logfile =~ ${FAULT_LOG_RE:-^$} ]]; then
  n=$(flock "$FAULT_DIR/lock" bash -c 'n=$(( $(cat "$0" 2>/dev/null || echo 0) + 1 )); echo "$n" > "$0"; echo "$n"' "$FAULT_DIR/count")
  at=${FAULT_AT:-1} count=${FAULT_COUNT:-1}
  if (( n >= at && n < at + count )); then
    printf '%s launch=%s kind=%s log=%s\n' "$(date +%s.%N)" "$n" "$kind" "$logfile" >> "$FAULT_DIR/faults.txt"
    case $kind in
      exit1) exit 1 ;;
      vanish) : > "$logfile"; exit 0 ;;
      hang) exec sleep 86407 ;;
      badport)
        bad=$(python3 -c '
import base64, sys
b = bytearray(base64.urlsafe_b64decode(sys.argv[1] + "=" * (-len(sys.argv[1]) % 4)))
b[-4:] = (12345).to_bytes(4, "big")
print(base64.urlsafe_b64encode(bytes(b)).decode())' "$service")
        for ((i = 0; i < ${#args[@]}; i++)); do
          [[ ${args[i]} == --service ]] && args[i + 1]=$bad
        done
        exec "${args[@]}" ;;
      *) echo "inject.sh: unknown FAULT_KIND $kind" >&2; exit 2 ;;
    esac
  fi
fi
exec "${args[@]}"
