// raft/logging.go
package raft

import (
	"fmt"
	"log"
	"time"
)

// LogLevel represents the logging level
type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
)

// Logger provides structured logging for Raft
type Logger struct {
	nodeID string
	level  LogLevel
}

// NewLogger creates a new logger for a Raft node
func NewLogger(nodeID string, level LogLevel) *Logger {
	return &Logger{
		nodeID: nodeID,
		level:  level,
	}
}

func (l *Logger) Debug(format string, args ...interface{}) {
	if l.level <= DEBUG {
		l.log("DEBUG", format, args...)
	}
}

func (l *Logger) Info(format string, args ...interface{}) {
	if l.level <= INFO {
		l.log("INFO", format, args...)
	}
}

func (l *Logger) Warn(format string, args ...interface{}) {
	if l.level <= WARN {
		l.log("WARN", format, args...)
	}
}

func (l *Logger) Error(format string, args ...interface{}) {
	if l.level <= ERROR {
		l.log("ERROR", format, args...)
	}
}

func (l *Logger) log(level, format string, args ...interface{}) {
	timestamp := time.Now().Format("15:04:05.000")
	prefix := fmt.Sprintf("[%s] [%s] [%s] ", timestamp, l.nodeID, level)
	log.Printf(prefix+format, args...)
}

// Specialized log functions for Raft events

func (l *Logger) LogStateChange(oldState, newState NodeState, term uint64) {
	emoji := map[NodeState]string{
		Follower:  "👤",
		Candidate: "🗳️",
		Leader:    "👑",
	}
	l.Info("%s %s → %s %s (term=%d)",
		emoji[oldState], oldState,
		emoji[newState], newState, term)
}

func (l *Logger) LogElectionStart(term uint64) {
	l.Info("🗳️  Starting election for term %d", term)
}

func (l *Logger) LogElectionWon(term, votes, needed uint64) {
	l.Info("👑 WON election for term %d (votes=%d/%d)", term, votes, needed)
}

func (l *Logger) LogElectionLost(term, votes, needed uint64) {
	l.Info("❌ LOST election for term %d (votes=%d/%d)", term, votes, needed)
}

func (l *Logger) LogVoteGranted(candidateID string, term uint64) {
	l.Info("✅ Granted vote to %s for term %d", candidateID, term)
}

func (l *Logger) LogVoteDenied(candidateID string, term uint64, reason string) {
	l.Info("❌ Denied vote to %s for term %d: %s", candidateID, term, reason)
}

func (l *Logger) LogHeartbeatSent(term uint64, peerCount int) {
	l.Debug("💓 Sent heartbeat to %d peers (term=%d)", peerCount, term)
}

func (l *Logger) LogHeartbeatReceived(leaderID string, term uint64) {
	l.Debug("💓 Received heartbeat from %s (term=%d)", leaderID, term)
}

func (l *Logger) LogAppendEntries(leaderID string, term, prevLogIndex uint64, entryCount int) {
	l.Debug("📥 Received AppendEntries from %s (term=%d, prevIndex=%d, entries=%d)",
		leaderID, term, prevLogIndex, entryCount)
}

func (l *Logger) LogCommit(index, term uint64) {
	l.Info("✅ Committed entry at index=%d (term=%d)", index, term)
}

func (l *Logger) LogApply(index uint64, command string) {
	l.Info("⚡ Applied command at index=%d: %s", index, command)
}

func (l *Logger) LogStepDown(oldTerm, newTerm uint64) {
	l.Info("⬇️  Stepping down: term %d → %d", oldTerm, newTerm)
}

func (l *Logger) LogElectionTimeout() {
	l.Debug("⏰ Election timeout - becoming candidate")
}

func (l *Logger) LogElectionTimerReset(reason string) {
	l.Debug("🔄 Election timer reset: %s", reason)
}
