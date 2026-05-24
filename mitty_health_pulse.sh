#!/bin/bash
# mitty_health_pulse.sh — Mitty Health Pulse
# Checks: RAM%, Disk%, Session size, BAK count
# Alert thresholds: RAM>75%, Disk>85%, Sessions>100MB
# Writes to ~/.cache/health_pulse.log

LOGFILE="/home/mathew/.cache/health_pulse.log"
TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S %Z")

# RAM
RAM_TOTAL=$(free -m | awk '/^Mem:/{print $2}')
RAM_USED=$(free -m | awk '/^Mem:/{print $3}')
RAM_PCT=$(( (RAM_USED * 100) / RAM_TOTAL ))

# Disk (root)
DISK_PCT=$(df -h / | awk 'NR==2{gsub("%","",$5); print $5}')

# Session size
SESSION_SIZE=$(du -sh /home/mathew/.openclaw/agents/main/sessions/ 2>/dev/null | awk '{print $1}')

# BAK count
BAK_COUNT=$(find /home/mathew/.openclaw/agents/main/sessions/ -name "*.bak-*" 2>/dev/null | wc -l)

# Thresholds
RAM_WARN=0
DISK_WARN=0
SESSION_WARN=0

[ "$RAM_PCT" -gt 75 ] && RAM_WARN=1
[ "$DISK_PCT" -gt 85 ] && DISK_WARN=1
# Sessions > 100MB — parse "331M" or "1.2G"
SESSION_BYTES=$(du -sb /home/mathew/.openclaw/agents/main/sessions/ 2>/dev/null | awk '{print $1}')
[ "$SESSION_BYTES" -gt 104857600 ] && SESSION_WARN=1

# Build status line
if [ "$RAM_WARN" -eq 1 ] || [ "$DISK_WARN" -eq 1 ] || [ "$SESSION_WARN" -eq 1 ]; then
  STATUS="HEALTH_CRITICAL"
  ALERT_FLAG="⚠️"
else
  STATUS="HEALTHY"
  ALERT_FLAG="✅"
fi

# Write to log
echo "# $TIMESTAMP" >> "$LOGFILE"
echo "STATUS=$STATUS RAM=${RAM_PCT}% DISK=${DISK_PCT}% SESSIONS=${SESSION_SIZE} BAK_COUNT=${BAK_COUNT}" >> "$LOGFILE"
echo "RAM_WARN=$RAM_WARN DISK_WARN=$DISK_WARN SESSION_WARN=$SESSION_WARN" >> "$LOGFILE"

# Trim log to last 200 lines
tail -200 "$LOGFILE" > "${LOGFILE}.tmp" && mv "${LOGFILE}.tmp" "$LOGFILE"

echo "[$STATUS] RAM=${RAM_PCT}% | DISK=${DISK_PCT}% | SESSIONS=${SESSION_SIZE} | BAK=${BAK_COUNT} | $ALERT_FLAG"