package line_reversal

import (
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/caarlos0/log"
	cmap "github.com/orcaman/concurrent-map/v2"
)

func New(conn *net.UDPConn) *LRServer {
	lrs := &LRServer{
		sessions: cmap.New[*LRSession](),
	}
	go lrs.Reaper(conn)
	return lrs
}

func (lrs *LRServer) Reaper(conn *net.UDPConn) {
	log.Info("Starting Reaper")
	for {
		time.Sleep(3 * time.Second)
		lrs.sessions.IterCb(func(key string, session *LRSession) {
			session.Unacked.IterCb(func(key string, msg *LRMessage) {
				if msg.Resends >= 20 {
					log.WithField("session", session.SessionID).Warn("Closing session due to too many resends")
					session.Unacked.Remove(key)
				} else {
					log.WithFields(log.Fields{"session": session.SessionID, "ordinal": msg.Ordinal, "resends": msg.Resends}).Info("Resending message")
					msg.Resends++
					conn.WriteToUDP([]byte(msg.EncodeMessage()), session.IP)
				}
			})
		})
	}
}

type LRServer struct {
	sessions cmap.ConcurrentMap[string, *LRSession]
}

func (lrs *LRServer) HandleRequest(conn *net.UDPConn, buffer []byte, ip *net.UDPAddr) []string {

	msg := ParseMessage(string(buffer))

	// if nil, then message parsing returned nil, meaning it hit an invalid message, return nothing.
	if msg == nil {
		return []string{}
	}

	switch msg.Command {
	case "connect":
		_, found := lrs.sessions.Get(msg.SessionID)
		if found {
			log.WithField("session", msg.SessionID).Info("Session already exists")
			ack := &LRMessage{
				Command:   "ack",
				SessionID: msg.SessionID,
			}
			return []string{ack.EncodeMessage()}
		} else {
			log.WithField("session", msg.SessionID).Info("New session")
			session := LRSession{
				IP:        ip,
				SessionID: msg.SessionID,
				Unacked:   cmap.New[*LRMessage](),
				Acked:     cmap.New[*LRMessage](),
			}
			lrs.sessions.Set(msg.SessionID, &session)
			ack := &LRMessage{
				Command:   "ack",
				SessionID: msg.SessionID,
			}
			return []string{ack.EncodeMessage()}
		}
	case "ack":
		session, found := lrs.sessions.Get(msg.SessionID)
		if !found {
			log.WithField("session", msg.SessionID).Info("Session not found")
			close := &LRMessage{
				Command:   "close",
				SessionID: msg.SessionID,
			}
			return []string{close.EncodeMessage()}
		}

		// If ordinal is less then the largest Acked, means its probably a duplicate that was delayed.
		if msg.Ordinal < session.GetLargestAcked() {
			log.WithField("session", msg.SessionID).Info("Duplicate ack")
			return []string{}
		}

		// If the ordinal is less than the bytes we've sent so far, then resend missing data.
		if msg.Ordinal < session.BytesSent {
			resp := &LRMessage{
				Command:   "data",
				SessionID: msg.SessionID,
				Ordinal:   msg.Ordinal,
				Data:      session.Data[msg.Ordinal:],
			}
			return []string{resp.EncodeMessage()}
		}

		// If the ordinal is greater than the bytes we've sent so far, client is misbehaving.
		if msg.Ordinal > session.BytesSent {
			lrs.sessions.Remove(msg.SessionID)
			resp := &LRMessage{
				Command:   "close",
				SessionID: msg.SessionID,
			}
			return []string{resp.EncodeMessage()}
		}

		// Update last message time for use in reaper/resender
		session.LastMessageTime = time.Now().UTC().Unix()
		toRemove := []*LRMessage{}
		// Move any messages with a lower ordinal than the ack message to the acked map.
		// Use 2 for loops to not affect the iteration of the map.
		for unacked := range session.Unacked.IterBuffered() {
			if unacked.Val.Ordinal <= msg.Ordinal {
				toRemove = append(toRemove, unacked.Val)
			}
		}
		for _, unacked := range toRemove {
			s, found := session.Unacked.Get(unacked.SessionID)
			session.Unacked.Remove(unacked.SessionID)
			if found {
				session.Acked.Set(unacked.SessionID, s)
			}
		}

	case "data":
		session, found := lrs.sessions.Get(msg.SessionID)
		if !found {
			log.WithField("session", msg.SessionID).Info("Session not found")
			close := &LRMessage{
				Command:   "close",
				SessionID: msg.SessionID,
			}
			return []string{close.EncodeMessage()}
		}

		if msg.Ordinal < session.BytesSent {
			log.WithField("session", msg.SessionID).Info("Duplicate data")
			return []string{}
		}

		if int(msg.Ordinal) > len(session.Data) {
			ack := &LRMessage{
				Command:   "ack",
				SessionID: msg.SessionID,
				Ordinal:   uint32(len(session.Data)),
			}
			return []string{ack.EncodeMessage()}
		}

		if len(msg.Data)+int(msg.Ordinal) < len(session.Data) {
			return []string{}
		}

		startPos := len(session.Data) - int(msg.Ordinal)
		trimmedData := msg.Data[startPos:]
		session.Data += trimmedData

		responses := []string{}
		ack := &LRMessage{
			Command:   "ack",
			SessionID: msg.SessionID,
			Ordinal:   uint32(len(session.Data)),
		}

		responses = append(responses, ack.EncodeMessage())

		if len(session.Data) > 0 && session.Data[len(session.Data)-1:] == "\n" {
			session.ReverseMessage()

			chunks := chunks(session.RData, 789)
			log.WithFields(log.Fields{"chunks": len(chunks), "bytesSent": session.BytesSent}).Info("Total chunks")

			if len(session.RData) < 789 {
				data := &LRMessage{
					Command:   "data",
					SessionID: msg.SessionID,
					Ordinal:   session.BytesSent,
					Data:      session.RData[session.BytesSent:],
				}
				session.BytesSent = uint32(len(session.RData))
				session.Unacked.Set(msg.SessionID, data)
				responses = append(responses, data.EncodeMessage())
			} else {
				for _, chunk := range chunks {
					data := &LRMessage{
						Command:   "data",
						SessionID: msg.SessionID,
						Ordinal:   session.BytesSent,
						Data:      chunk,
					}
					session.BytesSent += uint32(len(chunk))
					session.Unacked.Set(msg.SessionID, data)
					responses = append(responses, data.EncodeMessage())
				}
			}
		}
		return responses
	case "close":
		lrs.sessions.Remove(msg.SessionID)
		log.WithField("sessions", lrs.sessions.Count()).Info("Session closed")
		log.WithField("session", msg.SessionID).Info("Session closed")
		return []string{msg.EncodeMessage()}

	default:
		return []string{}
	}

	return []string{}
}

func (s *LRSession) ReverseMessage() {
	log.Info("Reversing message")
	splitMsg := strings.Split(s.Data, "\n")
	for msg := range splitMsg {
		splitMsg[msg] = Reverse(splitMsg[msg])
	}
	s.RData = strings.Join(splitMsg[:len(splitMsg)-1], "\n") + "\n"
}

type LRMessage struct {
	Command   string
	SessionID string
	Ordinal   uint32
	Data      string
	Resends   uint32
}

type LRSession struct {
	IP              *net.UDPAddr
	SessionID       string
	Data            string
	RData           string
	BytesSent       uint32
	LastMessageTime int64
	Unacked         cmap.ConcurrentMap[string, *LRMessage]
	Acked           cmap.ConcurrentMap[string, *LRMessage]
}

func (message *LRMessage) EncodeMessage() string {
	switch message.Command {
	case "connect":
		return fmt.Sprintf("/%s/%s/", message.Command, message.SessionID)
	case "ack":
		return fmt.Sprintf("/%s/%s/%d/", message.Command, message.SessionID, message.Ordinal)
	case "data":
		msg := strings.ReplaceAll(message.Data, "\\", "\\\\")
		msg = strings.ReplaceAll(msg, "/", "\\/")
		return fmt.Sprintf("/%s/%s/%d/%s/", message.Command, message.SessionID, message.Ordinal, msg)
	case "close":
		return fmt.Sprintf("/%s/%s/", message.Command, message.SessionID)
	default:
		return ""
	}
}

func (session *LRSession) GetLargestAcked() uint32 {
	largest := uint32(0)
	for acked := range session.Acked.IterBuffered() {
		if acked.Val.Ordinal > largest {
			largest = acked.Val.Ordinal
		}
	}
	return largest
}

func ParseMessage(message string) *LRMessage {
	message = strings.Trim(message, "\x00")

	if len(message) < 3 {
		return nil
	}

	if message[0] != '/' || message[len(message)-1:] != "/" {
		log.WithFields(log.Fields{"message": message, "len": len(message), "first": string(message[0]), "last": message[len(message)-1:]}).Info("Not Valid Message")
		return nil
	}

	message = message[1 : len(message)-1]

	splitMsg := strings.SplitN(message, "/", 4)

	// Not enough segments
	if len(splitMsg) < 2 {
		return nil
	}

	// /connect/SESSION/ or /close/SESSION/
	if len(splitMsg) == 2 {
		if splitMsg[0] != "connect" && splitMsg[0] != "close" {
			return nil
		}
		return &LRMessage{
			Command:   splitMsg[0],
			SessionID: splitMsg[1],
		}
	}

	// /ack/SESSION/ORDINAL/
	if len(splitMsg) == 3 {
		if splitMsg[0] != "ack" {
			return nil
		}

		position, err := strconv.Atoi(splitMsg[2])
		if err != nil {
			log.WithFields(log.Fields{"error": err, "strPosition": splitMsg[2], "split": splitMsg}).Error("Error parsing position")
			return nil
		}
		return &LRMessage{
			Command:   splitMsg[0],
			SessionID: splitMsg[1],
			Ordinal:   uint32(position),
		}
	}

	// /data/SESSION/ORDINAL/DATA/
	if len(splitMsg) == 4 {
		if splitMsg[0] != "data" {
			return nil
		}

		position, err := strconv.Atoi(splitMsg[2])
		if err != nil {
			log.WithFields(log.Fields{"error": err, "strPosition": splitMsg[2], "split": splitMsg[:len(splitMsg)-1]}).Error("Error parsing position")
			return nil
		}

		regexp := regexp.MustCompile(`(?:[^\\])\/`)
		if regexp.MatchString(splitMsg[3]) {
			return nil
		}

		msg := strings.ReplaceAll(splitMsg[3], "\\\\", "\\")
		msg = strings.ReplaceAll(msg, "\\/", "/")
		log.Info("Returning data message")
		return &LRMessage{
			Command:   splitMsg[0],
			SessionID: splitMsg[1],
			Ordinal:   uint32(position),
			Data:      msg,
		}
	}

	return nil
}

func Reverse(s string) string {
	n := len(s)
	runes := make([]rune, n)
	for _, rune := range s {
		n--
		runes[n] = rune
	}
	return string(runes[n:])
}

func chunks(s string, chunkSize int) []string {
	if len(s) == 0 {
		return nil
	}
	if chunkSize >= len(s) {
		return []string{s}
	}
	chunks := []string{}
	numFullChunks := len(s) / chunkSize
	remainder := len(s) % chunkSize
	for i := 0; i < numFullChunks; i++ {
		chunks = append(chunks, s[i*chunkSize:(i+1)*chunkSize])
	}
	chunks = append(chunks, s[numFullChunks*chunkSize:numFullChunks*chunkSize+remainder])
	return chunks
}
