#!/bin/bash
# mitty_security_audit.sh — Mitty Security Audit
# Checks: SSH, Firewall, Gateway, Cron, RAM, Filesystem
# Writes results to memory/YYYY-MM-DD.md

AUDIT_DATE=$(date "+%Y-%m-%d")
MEMORY_DIR="/home/mathew/.openclaw/workspace/memory"
AUDIT_LOG="$MEMORY_DIR/${AUDIT_DATE}-security-audit.md"
TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S %Z")

# Ensure memory dir exists
mkdir -p "$MEMORY_DIR"

# Initialize report
echo "# Security Audit — $TIMESTAMP" > "$AUDIT_LOG"
echo "" >> "$AUDIT_LOG"

# ── 1. SSH Check ──────────────────────────────────────────────
echo "## 1. SSH Security Check" >> "$AUDIT_LOG"
SSH_STATUS="PASS"
SSH_ISSUES=""

# Check SSH config permissions
if [ -f /etc/ssh/sshd_config ]; then
  PERMS=$(stat -c "%a" /etc/ssh/sshd_config 2>/dev/null)
  if [ "$PERMS" != "644" ] && [ "$PERMS" != "600" ]; then
    SSH_STATUS="WARN"
    SSH_ISSUES="$SSH_ISSUES sshd_config permissions=$PERMS (expected 644 or 600)"
  fi
fi

# Check if SSH is running
if pgrep -x sshd > /dev/null 2>&1; then
  SSH_RUNNING="YES"
  # Check for root login
  ROOT_LOGIN=$(grep -c "^PermitRootLogin yes" /etc/ssh/sshd_config 2>/dev/null || true)
  ROOT_LOGIN=${ROOT_LOGIN:-0}
  if [ "${ROOT_LOGIN:-0}" -gt 0 ]; then
    SSH_STATUS="FAIL"
    SSH_ISSUES="$SSH_ISSUES RootLogin enabled!"
  fi
  # Check password auth
  PWD_AUTH=$(grep -c "^PasswordAuthentication yes" /etc/ssh/sshd_config 2>/dev/null || true)
  PWD_AUTH=${PWD_AUTH:-0}
  if [ "${PWD_AUTH:-0}" -gt 0 ]; then
    SSH_STATUS="WARN"
    SSH_ISSUES="$SSH_ISSUES PasswordAuthentication enabled"
  fi
else
  SSH_RUNNING="NO"
fi

echo "- SSH Daemon Running: $SSH_RUNNING" >> "$AUDIT_LOG"
echo "- Root Login: $([ "$ROOT_LOGIN" -gt 0 ] && echo 'ENABLED ⚠️' || echo 'disabled ✅')" >> "$AUDIT_LOG"
echo "- Password Auth: $([ "$PWD_AUTH" -gt 0 ] && echo 'ENABLED ⚠️' || echo 'disabled ✅')" >> "$AUDIT_LOG"
echo "- Status: **$SSH_STATUS**" >> "$AUDIT_LOG"
[ -n "$SSH_ISSUES" ] && echo "  - Issues: $SSH_ISSUES" >> "$AUDIT_LOG"
echo "" >> "$AUDIT_LOG"

# ── 2. Firewall Check ─────────────────────────────────────────
echo "## 2. Firewall Check" >> "$AUDIT_LOG"
FW_STATUS="PASS"
FW_ISSUES=""

# UFW status
if command -v ufw &> /dev/null; then
  UFW_ACTIVE=$(ufw status 2>/dev/null | grep -c "Status: active" || echo 0)
  if [ "$UFW_ACTIVE" -gt 0 ]; then
    echo "- UFW: **ACTIVE ✅**" >> "$AUDIT_LOG"
    echo "  Rules:" >> "$AUDIT_LOG"
    ufw status numbered 2>/dev/null | grep -E "^\[" >> "$AUDIT_LOG"
  else
    echo "- UFW: INACTIVE ⚠️" >> "$AUDIT_LOG"
    FW_STATUS="WARN"
    FW_ISSUES="$FW_ISSUES UFW not active"
  fi
else
  echo "- UFW: **NOT INSTALLED**" >> "$AUDIT_LOG"
  FW_STATUS="WARN"
  FW_ISSUES="$FW_ISSUES UFW not installed"
fi

# iptables basic check
if command -v iptables &> /dev/null; then
  IPT_RULES=$(iptables -L -n 2>/dev/null | wc -l)
  echo "- iptables rules loaded: $IPT_RULES lines" >> "$AUDIT_LOG"
  # Check for default drop
  INPUT_POLICY=$(iptables -L INPUT 2>/dev/null | grep "Chain INPUT" | awk '{print $4}' | tr -d ')')
  echo "- INPUT default policy: **$INPUT_POLICY**" >> "$AUDIT_LOG"
  if [ "$INPUT_POLICY" = "ACCEPT" ]; then
    FW_STATUS="WARN"
    FW_ISSUES="$FW_ISSUES INPUT policy is ACCEPT (should be DROP)"
  fi
fi

echo "- Status: **$FW_STATUS**" >> "$AUDIT_LOG"
[ -n "$FW_ISSUES" ] && echo "  - Issues: $FW_ISSUES" >> "$AUDIT_LOG"
echo "" >> "$AUDIT_LOG"

# ── 3. Gateway Check ──────────────────────────────────────────
echo "## 3. OpenClaw Gateway Check" >> "$AUDIT_LOG"
GW_STATUS="PASS"
GW_ISSUES=""

# Check if openclaw binary exists
if command -v openclaw &> /dev/null; then
  echo "- openclaw binary: **FOUND ✅** ($(openclaw --version 2>/dev/null || echo 'version unknown'))" >> "$AUDIT_LOG"
else
  echo "- openclaw binary: **NOT FOUND ⚠️**" >> "$AUDIT_LOG"
  GW_STATUS="FAIL"
  GW_ISSUES="$GW_ISSUES openclaw binary not in PATH"
fi

# Check gateway status
GW_OUTPUT=$(openclaw gateway status 2>&1)
if echo "$GW_OUTPUT" | grep -qi "running\|active"; then
  echo "- Gateway: **RUNNING ✅**" >> "$AUDIT_LOG"
elif echo "$GW_OUTPUT" | grep -qi "stopped\|inactive\|dead"; then
  echo "- Gateway: **STOPPED ⚠️**" >> "$AUDIT_LOG"
  GW_STATUS="WARN"
  GW_ISSUES="$GW_ISSUES Gateway not running"
else
  echo "- Gateway: **UNKNOWN ⚠️**" >> "$AUDIT_LOG"
  echo "  Output: $GW_OUTPUT" >> "$AUDIT_LOG"
  GW_STATUS="WARN"
fi

# Check gateway config
GW_CONFIG="/home/matatheW/.openclaw/gateway.yml"
if [ -f "$GW_CONFIG" ]; then
  echo "- Config file: **EXISTS ✅**" >> "$AUDIT_LOG"
else
  echo "- Config file: **NOT FOUND ⚠️**" >> "$AUDIT_LOG"
fi

echo "- Status: **$GW_STATUS**" >> "$AUDIT_LOG"
[ -n "$GW_ISSUES" ] && echo "  - Issues: $GW_ISSUES" >> "$AUDIT_LOG"
echo "" >> "$AUDIT_LOG"

# ── 4. Cron Health ─────────────────────────────────────────────
echo "## 4. Cron Health Check" >> "$AUDIT_LOG"
CRON_STATUS="PASS"
CRON_ISSUES=""

CRON_RUNNING=$(pgrep -c cron 2>/dev/null || pgrep -c crond 2>/dev/null || echo 0)
if [ "$CRON_RUNNING" -gt 0 ]; then
  echo "- Cron/crond: **RUNNING ✅** (pid count: $CRON_RUNNING)" >> "$AUDIT_LOG"
else
  echo "- Cron/crond: **NOT RUNNING ⚠️**" >> "$AUDIT_LOG"
  CRON_STATUS="WARN"
  CRON_ISSUES="$CRON_ISSUES cron daemon not running"
fi

# List user crons
USER_CRONS=$(crontab -l 2>/dev/null | grep -v "^#" | grep -v "^$" | wc -l)
echo "- User crontab entries: $USER_CRONS" >> "$AUDIT_LOG"
if [ "$USER_CRONS" -gt 0 ]; then
  echo "  Crontab:" >> "$AUDIT_LOG"
  crontab -l 2>/dev/null | grep -v "^#" | grep -v "^$" | sed 's/^/    /' >> "$AUDIT_LOG"
fi

# Check system crons
SYS_CRON_COUNT=$(find /etc/cron.d /etc/cron.daily /etc/cron.hourly /etc/cron.monthly -type f 2>/dev/null | wc -l)
echo "- System cron entries: $SYS_CRON_COUNT" >> "$AUDIT_LOG"

echo "- Status: **$CRON_STATUS**" >> "$AUDIT_LOG"
[ -n "$CRON_ISSUES" ] && echo "  - Issues: $CRON_ISSUES" >> "$AUDIT_LOG"
echo "" >> "$AUDIT_LOG"

# ── 5. RAM Check ───────────────────────────────────────────────
echo "## 5. RAM Check" >> "$AUDIT_LOG"
RAM_TOTAL=$(free -m | awk '/^Mem:/{print $2}')
RAM_USED=$(free -m | awk '/^Mem:/{print $3}')
RAM_PCT=$(( (RAM_USED * 100) / RAM_TOTAL ))
RAM_AVAIL=$(free -m | awk '/^Mem:/{print $7}')

echo "- Total: ${RAM_TOTAL}MB | Used: ${RAM_USED}MB | Available: ${RAM_AVAIL}MB" >> "$AUDIT_LOG"
echo "- Usage: **${RAM_PCT}%**" >> "$AUDIT_LOG"

if [ "$RAM_PCT" -gt 90 ]; then
  RAM_STATUS="CRITICAL"
  echo "- Status: **CRITICAL ⚠️⚠️** (>90%)" >> "$AUDIT_LOG"
elif [ "$RAM_PCT" -gt 75 ]; then
  RAM_STATUS="WARN"
  echo "- Status: **WARN ⚠️** (>75%)" >> "$AUDIT_LOG"
else
  RAM_STATUS="PASS"
  echo "- Status: **PASS ✅**" >> "$AUDIT_LOG"
fi
echo "" >> "$AUDIT_LOG"

# ── 6. Filesystem Check ────────────────────────────────────────
echo "## 6. Filesystem Check" >> "$AUDIT_LOG"
FS_STATUS="PASS"
FS_ISSUES=""

df_output=$(df -h 2>/dev/null)
echo "$df_output" | tail -n +2 | while read -r line; do
  FS=$(echo "$line" | awk '{print $1}')
  SIZE=$(echo "$line" | awk '{print $2}')
  USE_PCT=$(echo "$line" | awk '{gsub("%","",$5); print $5}')
  MOUNT=$(echo "$line" | awk '{print $6}')

  if [ "$USE_PCT" -ge 90 ]; then
    echo "- **$MOUNT** ($FS ${SIZE}): **${USE_PCT}% ⚠️⚠️**" >> "$AUDIT_LOG"
    FS_STATUS="CRITICAL"
    FS_ISSUES="$FS_ISSUES $MOUNT at ${USE_PCT}%"
  elif [ "$USE_PCT" -ge 80 ]; then
    echo "- **$MOUNT** ($FS ${SIZE}): **${USE_PCT}% ⚠️**" >> "$AUDIT_LOG"
    [ "$FS_STATUS" = "PASS" ] && FS_STATUS="WARN"
  else
    echo "- $MOUNT ($FS ${SIZE}): ${USE_PCT}% ✅" >> "$AUDIT_LOG"
  fi
done

# Check workspace permissions
WORKSPACE="/home/mathew/.openclaw/workspace"
if [ -d "$WORKSPACE" ]; then
  WS_OWNER=$(stat -c "%U:%G" "$WORKSPACE" 2>/dev/null)
  WS_PERMS=$(stat -c "%a" "$WORKSPACE" 2>/dev/null)
  echo "- Workspace owner: $WS_OWNER | perms: $WS_PERMS" >> "$AUDIT_LOG"
  if [ "$WS_PERMS" -lt 750 ]; then
    FS_STATUS="WARN"
    FS_ISSUES="$FS_ISSUES workspace perms too open ($WS_PERMS)"
  fi
fi

echo "- Status: **$FS_STATUS**" >> "$AUDIT_LOG"
[ -n "$FS_ISSUES" ] && echo "  - Issues: $FS_ISSUES" >> "$AUDIT_LOG"
echo "" >> "$AUDIT_LOG"

# ── Summary ────────────────────────────────────────────────────
echo "## Summary" >> "$AUDIT_LOG"
OVERALL="PASS"
[ "$SSH_STATUS" = "FAIL" ] && OVERALL="FAIL"
[ "$FW_STATUS" = "FAIL" ] || [ "$GW_STATUS" = "FAIL" ] || [ "$CRON_STATUS" = "FAIL" ] || [ "$FS_STATUS" = "CRITICAL" ] && [ "$OVERALL" != "FAIL" ] && OVERALL="WARN"
[ "$SSH_STATUS" = "WARN" ] || [ "$FW_STATUS" = "WARN" ] || [ "$GW_STATUS" = "WARN" ] || [ "$CRON_STATUS" = "WARN" ] || [ "$RAM_STATUS" = "WARN" ] || [ "$FS_STATUS" = "WARN" ] && [ "$OVERALL" = "PASS" ] && OVERALL="WARN"

if [ "$OVERALL" = "PASS" ]; then
  echo "**Overall: ✅ PASS** — All checks healthy" >> "$AUDIT_LOG"
elif [ "$OVERALL" = "WARN" ]; then
  echo "**Overall: ⚠️ WARN** — Some areas need attention" >> "$AUDIT_LOG"
else
  echo "**Overall: ❌ FAIL** — Critical issues detected!" >> "$AUDIT_LOG"
fi

echo "" >> "$AUDIT_LOG"
echo "---" >> "$AUDIT_LOG"
echo "*Audit completed at $TIMESTAMP* | SSH=$SSH_STATUS | FW=$FW_STATUS | GW=$GW_STATUS | CRON=$CRON_STATUS | RAM=${RAM_PCT}% | FS=$FS_STATUS" >> "$AUDIT_LOG"

# Print to stdout
echo "=== MITTY SECURITY AUDIT — $TIMESTAMP ==="
cat "$AUDIT_LOG"
